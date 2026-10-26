use crate::error::ReceivedClose;
use crate::{
    BasicProperties, Channel, ChannelState, Configuration, Connection, ConnectionState,
    ConnectionStatus, Error, ErrorKind, RemoteClose, Result,
    configuration::{NegotiatedConfig, RecoveryConfig},
    connection_closer::ConnectionCloser,
    events::{Events, EventsSender},
    frames::Frames,
    id_sequence::IdSequence,
    internal_rpc::InternalRPCHandle,
    killswitch::KillSwitch,
    protocol::{AMQPClass, AMQPError, AMQPHardError, channel, connection},
    socket_state::SocketStateHandle,
    types::{ChannelId, DeliveryTag, Identifier, PayloadSize},
};
use amq_protocol::frame::{AMQPFrame, ProtocolVersion};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::{debug, error, trace};

#[derive(Clone)]
pub(crate) struct Channels {
    ingress: Arc<Mutex<Ingress>>,
    inner: Arc<RwLock<Inner>>,
    channel0: Channel,
    configuration: Configuration,
    connection_status: ConnectionStatus,
    internal_rpc: InternalRPCHandle,
    frames: Frames,
    connection_killswitch: KillSwitch,
    events: Events,
}

#[derive(Default)]
struct Ingress {
    frozen: bool,
    high_water: BTreeMap<ChannelId, DeliveryTag>,
}

impl Channels {
    pub(crate) fn new(
        configuration: Configuration,
        connection_status: ConnectionStatus,
        waker: SocketStateHandle,
        internal_rpc: InternalRPCHandle,
        frames: Frames,
        events: Events,
    ) -> Self {
        let mut inner = Inner::new(
            configuration.negotiated_config.clone(),
            configuration.recovery_config(),
            waker,
            configuration.connection_close_observer(),
        );
        let channel0 = inner.create_channel(
            0,
            connection_status.clone(),
            internal_rpc.clone(),
            frames.clone(),
            events.sender(),
            None,
            None,
        );
        channel0.set_state(ChannelState::Connected);

        Self {
            ingress: Arc::default(),
            inner: Arc::new(RwLock::new(inner)),
            channel0,
            configuration,
            connection_status,
            internal_rpc,
            frames,
            connection_killswitch: KillSwitch::default(),
            events,
        }
    }

    pub(crate) fn create(
        &self,
        connection_closer: Arc<ConnectionCloser>,
        observer: Option<Arc<dyn crate::RemoteCloseObserver>>,
    ) -> Result<Channel> {
        let mut ingress = self.ingress.lock().unwrap_or_else(|e| e.into_inner());
        let channel = self.write().create(
            self.connection_status.clone(),
            self.internal_rpc.clone(),
            self.frames.clone(),
            self.events.sender(),
            connection_closer,
            observer,
        )?;
        // A reopened native id is a new channel lifetime with a new tag
        // sequence. Creation and ingress share the fence, so no first delivery
        // can race this reset, and a frozen retirement snapshot stays final.
        if !ingress.frozen {
            ingress.high_water.remove(&channel.id());
        }
        Ok(channel)
    }

    pub(crate) fn channel0(&self) -> Channel {
        self.channel0.clone()
    }

    pub(crate) fn can_recover(&self, error: &Error) -> bool {
        self.configuration.recovery_config().can_recover(error)
    }

    pub(crate) fn get(&self, id: ChannelId) -> Option<Channel> {
        if id == 0 {
            Some(self.channel0())
        } else {
            self.read().channels.get(&id).cloned()
        }
    }

    pub(crate) fn invalid_channel_error(&self, id: ChannelId) -> Error {
        Error::from(ErrorKind::InvalidChannel(id)).with_remote_close(
            self.frames
                .channel_poison(id)
                .and_then(|error| error.remote_close_shared())
                .or_else(|| self.connection_status.remote_close()),
        )
    }

    pub(crate) fn remove(&self, id: ChannelId, error: Error) -> Result<()> {
        let close = error.remote_close_shared();
        self.frames.clear_expected_replies(id, error);

        if id == 0 {
            return Ok(());
        }

        if self.write().channels.remove(&id).is_some() {
            Ok(())
        } else {
            Err(Error::from(ErrorKind::InvalidChannel(id))
                .with_remote_close(close)
                .with_remote_close(self.invalid_channel_error(id).remote_close_shared()))
        }
    }

    pub(crate) fn connection_killswitch(&self) -> KillSwitch {
        self.connection_killswitch.clone()
    }

    pub(crate) fn receive_method(&self, id: ChannelId, method: AMQPClass) -> Result<()> {
        if id == 0 {
            self.channel0.receive_method(method)
        } else {
            if let Some(channel) = self.get(id) {
                return channel.receive_method(method);
            }
            // Routing can reject a Close before a generated receiver is reached.
            let close = match method {
                AMQPClass::Connection(crate::protocol::connection::AMQPMethod::Close(close)) => {
                    Some(RemoteClose::Connection(close))
                }
                AMQPClass::Channel(crate::protocol::channel::AMQPMethod::Close(close)) => {
                    Some(RemoteClose::Channel(close))
                }
                _ => None,
            };
            Err(match close {
                Some(close) => {
                    let close = match self.connection_status.remote_close() {
                        Some(existing) => existing,
                        None => {
                            let context = if matches!(&close, RemoteClose::Connection(_)) {
                                self.configuration
                                    .connection_close_observer()
                                    .and_then(|observer| observer.received(&close, false))
                            } else {
                                None
                            };
                            Arc::new(ReceivedClose::new(close, context))
                        }
                    };
                    let close = self.connection_status.set_remote_close(close);
                    Error::from(ErrorKind::InvalidChannel(id)).with_remote_close(Some(close))
                }
                None => self.invalid_channel_error(id),
            })
        }
    }

    pub(crate) fn handle_content_header_frame(
        &self,
        id: ChannelId,
        class_id: Identifier,
        size: PayloadSize,
        properties: BasicProperties,
    ) -> Result<()> {
        self.get(id)
            .map(|channel| channel.handle_content_header_frame(class_id, size, properties))
            .unwrap_or_else(|| Err(self.invalid_channel_error(id)))
    }

    pub(crate) fn handle_body_frame(&self, id: ChannelId, payload: Vec<u8>) -> Result<()> {
        self.get(id)
            .map(|channel| channel.handle_body_frame(payload))
            .unwrap_or_else(|| Err(self.invalid_channel_error(id)))
    }

    pub(crate) fn set_connection_closing(&self) {
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            self.apply_connection_error(error);
            return;
        }
        self.connection_status.set_state(ConnectionState::Closing);
        for channel in self.read().channels.values() {
            channel.set_closing(None);
        }
    }

    // Stop new operations without settling existing waiters before the
    // rejection's close write. set_closing would cancel a partial basic.get.
    pub(crate) fn begin_frame_rejection(&self) {
        let _shutdown = self.connection_status.shutdown_guard();
        if self.connection_status.errored() {
            return;
        }
        self.connection_status.set_state(ConnectionState::Closing);
        for channel in self.read().channels.values() {
            channel.set_state(ChannelState::Closing);
        }
    }

    pub(crate) fn set_connection_closed(&self, error: Error) {
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            self.apply_connection_error(error);
            return;
        }
        let error = self.connection_status.retain_remote_close(error);
        self.frames.clear_all_expected_replies(error.clone());
        self.connection_status.set_state(ConnectionState::Closed);
        for channel in self.read().channels.values() {
            channel.set_closed(error.clone());
        }
    }

    pub(crate) fn should_init_connection_recovery(&self) -> bool {
        if self.connection_status.connected() {
            self.connection_killswitch.kill();
            return true;
        }
        false
    }

    pub(crate) fn set_connection_error(&self, error: Error) {
        let _shutdown = self.connection_status.shutdown_guard();
        self.apply_connection_error(self.connection_status.rejection().unwrap_or(error));
    }

    fn apply_connection_error(&self, error: Error) {
        let error = self.connection_status.retain_remote_close(error);
        // Do nothing if we were already in error
        if let ConnectionState::Error = self.connection_status.set_state(ConnectionState::Error) {
            return;
        }

        error!(%error, "Connection error");
        if let Some(resolver) = self.frames.connection_resolver(0) {
            resolver.reject(error.clone());
        }

        self.frames.drop_pending(error.clone(), &self.internal_rpc);
        self.events.sender().error(error.clone());
        for channel in self.read().channels.values() {
            channel.set_connection_error(error.clone());
        }
        if let Some(error) = self.connection_status.rejection()
            && let Some(handshake) = self.connection_status.defer_handshake_close(error)
        {
            handshake.finish_close();
        }
    }

    pub(crate) fn flow(&self) -> bool {
        self.read().channels.values().all(|c| c.status().flow())
    }

    pub(crate) fn handle_frame(&self, f: AMQPFrame) -> Result<()> {
        // The I/O loop's only native frame-dispatch entry point. Retain this
        // guard through content assembly and receiver completion, so a freeze
        // joins any in-flight handoff before returning the final tag snapshot.
        let mut ingress = self.ingress.lock().unwrap_or_else(|e| e.into_inner());
        if ingress.frozen {
            return Ok(());
        }
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            // Closing ignores ordinary input. A competing close may abandon
            // the write, but none of its native teardown effects may run.
            if matches!(
                f,
                AMQPFrame::Method(
                    _,
                    AMQPClass::Connection(
                        connection::AMQPMethod::Close(_) | connection::AMQPMethod::CloseOk(_)
                    )
                ) | AMQPFrame::Method(
                    _,
                    AMQPClass::Channel(
                        channel::AMQPMethod::Close(_) | channel::AMQPMethod::CloseOk(_)
                    )
                )
            ) {
                self.apply_connection_error(error.clone());
                return Err(error);
            }
            return Ok(());
        }
        if let AMQPFrame::Method(id, AMQPClass::Basic(method)) = &f {
            let tag = match method {
                crate::protocol::basic::AMQPMethod::Deliver(method) => Some(method.delivery_tag),
                crate::protocol::basic::AMQPMethod::GetOk(method) => Some(method.delivery_tag),
                _ => None,
            };
            if let Some(tag) = tag {
                let high_water = ingress.high_water.entry(*id).or_default();
                *high_water = (*high_water).max(tag);
            }
        }
        trace!(channel = f.channel_id(), "will handle frame");
        match f {
            AMQPFrame::ProtocolHeader(version) => {
                error!(
                    "we asked for AMQP {} but the server only supports AMQP {}",
                    ProtocolVersion::amqp_0_9_1(),
                    version
                );
                let error: Error = ErrorKind::InvalidProtocolVersion(version).into();
                if let Some(resolver) = self.frames.connection_resolver(0) {
                    resolver.reject(error.clone());
                }
                return Err(error);
            }
            AMQPFrame::Method(channel_id, method) => {
                self.receive_method(channel_id, method)?;
            }
            AMQPFrame::Heartbeat => {
                debug!("received heartbeat from server");
            }
            AMQPFrame::InvalidHeartbeat(channel_id) => {
                error!(channel=%channel_id, "received invalid heartbeat");
                let error = AMQPError::new(
                    AMQPHardError::FRAMEERROR.into(),
                    format!("heartbeat frame received on channel {channel_id}").into(),
                );
                return self.channel0.report_protocol_violation(error, 0, 0);
            }
            AMQPFrame::Header(channel_id, header) => {
                if channel_id == 0 {
                    error!(channel=%channel_id, "received content header");
                    let error = AMQPError::new(
                        AMQPHardError::CHANNELERROR.into(),
                        format!("content header frame received on channel {channel_id}").into(),
                    );
                    return self
                        .channel0
                        .report_protocol_violation(error, header.class_id, 0);
                } else {
                    self.handle_content_header_frame(
                        channel_id,
                        header.class_id,
                        header.body_size,
                        header.properties,
                    )?;
                }
            }
            AMQPFrame::Body(channel_id, payload) => {
                self.handle_body_frame(channel_id, payload)?;
            }
        }
        Ok(())
    }

    pub(crate) fn init_connection_recovery(&self, error: Error) -> Error {
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            self.apply_connection_error(error.clone());
            return error;
        }
        trace!("init connection recovery");
        let error = self.connection_status.retain_remote_close(error);
        self.connection_status.set_reconnecting();
        self.frames.clear_connection_steps(&error);
        let error = self
            .read()
            .channels
            .values()
            .fold(error, |error, channel| channel.init_recovery(error));
        self.channel0.init_recovery(error)
    }

    pub(crate) async fn start_recovery(&self) -> Result<()> {
        let channels = self.read().channels.values().cloned().collect::<Vec<_>>();

        self.connection_killswitch.reset();
        self.frames.drop_poison();
        self.channel0.update_recovery();
        self.channel0.finalize_connection();

        Connection::for_reconnect(
            self.configuration.clone(),
            self.connection_status.clone(),
            self.internal_rpc.clone(),
            self.events.clone(),
        )
        .start(self.channel0())
        .await?;

        trace!("Connection recovered, now recovering channels");

        // TODO: use future::join! when stable
        for channel in channels {
            channel.start_recovery().await?;
        }

        Ok(())
    }

    pub(crate) fn init_connection_shutdown(&self, error: Error) {
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            self.apply_connection_error(error);
            return;
        }
        let error = self.connection_status.retain_remote_close(error);
        self.frames.drop_pending(error, &self.internal_rpc);
    }

    pub(crate) fn finish_connection_shutdown(&self) {
        let _shutdown = self.connection_status.shutdown_guard();
        if let Some(error) = self.connection_status.rejection() {
            self.apply_connection_error(error);
        }
        self.connection_killswitch.kill();
    }

    pub(crate) fn freeze_ingress(&self) -> BTreeMap<ChannelId, DeliveryTag> {
        let mut ingress = self.ingress.lock().unwrap_or_else(|e| e.into_inner());
        ingress.frozen = true;
        ingress.high_water.clone()
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod ingress_tests;

impl fmt::Debug for Channels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Channels");
        if let Ok(inner) = self.inner.try_read() {
            debug
                .field("channels", &inner.channels.values())
                .field("channel_id", &inner.channel_id)
                .field("configuration", &inner.configuration);
        }
        debug
            .field("frames", &self.frames)
            .field("connection_status", &self.connection_status)
            .finish()
    }
}

struct Inner {
    connection_close_observer: Option<Arc<dyn crate::RemoteCloseObserver>>,
    channels: HashMap<ChannelId, Channel>,
    channel_id: IdSequence<ChannelId>,
    configuration: NegotiatedConfig,
    recovery_config: RecoveryConfig,
    waker: SocketStateHandle,
}

impl Inner {
    fn new(
        configuration: NegotiatedConfig,
        recovery_config: RecoveryConfig,
        waker: SocketStateHandle,
        connection_close_observer: Option<Arc<dyn crate::RemoteCloseObserver>>,
    ) -> Self {
        Self {
            connection_close_observer,
            channels: HashMap::default(),
            channel_id: IdSequence::new(false),
            configuration,
            recovery_config,
            waker,
        }
    }

    fn create_channel(
        &mut self,
        id: ChannelId,
        connection_status: ConnectionStatus,
        internal_rpc: InternalRPCHandle,
        frames: Frames,
        events_sender: EventsSender,
        connection_closer: Option<Arc<ConnectionCloser>>,
        observer: Option<Arc<dyn crate::RemoteCloseObserver>>,
    ) -> Channel {
        debug!(%id, "create channel");
        Channel::new(
            id,
            self.configuration.clone(),
            connection_status,
            self.waker.clone(),
            internal_rpc,
            frames,
            connection_closer,
            self.recovery_config,
            events_sender,
            self.connection_close_observer.clone(),
            observer,
        )
    }

    fn create(
        &mut self,
        connection_status: ConnectionStatus,
        internal_rpc: InternalRPCHandle,
        frames: Frames,
        events_sender: EventsSender,
        connection_closer: Arc<ConnectionCloser>,
        observer: Option<Arc<dyn crate::RemoteCloseObserver>>,
    ) -> Result<Channel> {
        debug!("create channel");
        self.channel_id.set_max(self.configuration.channel_max());
        if self.channels.len() >= self.configuration.channel_max() as usize {
            return Err(ErrorKind::ChannelsLimitReached.into());
        }
        let first_id = self.channel_id.next();
        let mut id = first_id;
        let mut met_first_id = false;
        loop {
            if id == first_id {
                if met_first_id {
                    break;
                }
                met_first_id = true;
            }
            if !self.channels.contains_key(&id) {
                let channel = self.create_channel(
                    id,
                    connection_status,
                    internal_rpc,
                    frames,
                    events_sender,
                    Some(connection_closer),
                    observer,
                );
                self.channels.insert(id, channel.clone_internal());
                return Ok(channel);
            }
            id = self.channel_id.next();
        }
        Err(ErrorKind::ChannelsLimitReached.into())
    }
}
