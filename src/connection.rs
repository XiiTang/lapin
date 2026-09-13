use crate::{
    ConnectionProperties, ConnectionStatus, Error, Event, Promise, Result,
    channel::{Channel, Reply},
    channels::Channels,
    configuration::Configuration,
    connection_closer::ConnectionCloser,
    connection_step::ConnectionStep,
    events::Events,
    frames::{ExpectedReply, Frames},
    heartbeat::Heartbeat,
    internal_rpc::{InternalRPC, InternalRPCHandle},
    io_loop::IoLoop,
    runtime,
    secret_update::SecretUpdate,
    socket_state::SocketState,
    tcp::{AMQPUriTcpExt, OwnedTLSConfig},
    thread::ThreadHandle,
    types::{LongString, ReplyCode, ShortString},
    uri::AMQPUri,
};
use amq_protocol::frame::{AMQPFrame, ProtocolVersion};
use async_rs::{Runtime, traits::*};
use async_trait::async_trait;
use futures_core::Stream;
use std::{fmt, sync::Arc};
use tracing::trace;

/// A TCP connection to the AMQP server.
///
/// To connect to the server, one of the [`connect`](Connection::connect) methods has to be called.
///
/// Afterwards, create a [`Channel`] by calling [`create_channel`](Connection::create_channel).
///
/// Also see the RabbitMQ documentation on [connections](https://www.rabbitmq.com/connections.html).
pub struct Connection {
    configuration: Configuration,
    status: ConnectionStatus,
    internal_rpc: InternalRPCHandle,
    events: Events,
    io_loop: ThreadHandle,
    closer: Arc<ConnectionCloser>,
}

struct ConnectorControl {
    force_stop: crate::killswitch::KillSwitch,
    channels: Channels,
    thread: ThreadHandle,
    waker: crate::socket_state::SocketStateHandle,
    rpc: InternalRPCHandle,
    tasks: crate::task_scope::TaskScope,
}
impl ConnectorControl {
    fn stop(&self) {
        if self.force_stop.kill() {
            self.channels.set_connection_error(
                std::io::Error::from(std::io::ErrorKind::ConnectionAborted).into(),
            );
            self.tasks.stop();
            self.rpc.stop();
            self.waker.wake();
        }
    }
}
/// Owner of the physical driver created by `Connection::from_stream`.
/// Stop and join it before dropping the supplied async task scope. Dropping
/// this owner requests an immediate stop; it never emits AMQP cleanup commands.
pub struct ConnectionDriver(ConnectorControl);
impl ConnectionDriver {
    /// Stop physical I/O immediately without sending protocol cleanup.
    pub fn stop(&self) {
        self.0.stop();
    }
    /// Clone a stop callback for an external cancellation/deadline watcher.
    pub fn stop_handle(&self) -> impl Fn() + Clone + Send + Sync + 'static {
        let force = self.0.force_stop.clone();
        let channels = self.0.channels.clone();
        let tasks = self.0.tasks.clone();
        let rpc = self.0.rpc.clone();
        let waker = self.0.waker.clone();
        move || {
            if force.kill() {
                channels.set_connection_error(
                    std::io::Error::from(std::io::ErrorKind::ConnectionAborted).into(),
                );
                tasks.stop();
                rpc.stop();
                waker.wake();
            }
        }
    }
    /// Whether physical I/O and all scoped async tasks have exited.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.0.thread.is_finished() && self.0.tasks.is_finished()
    }
    /// Join physical I/O and all scoped async tasks. Call on a blocking worker after stop.
    pub fn join(self) -> Result<()> {
        let result = self.0.thread.wait("supplied AMQP driver");
        self.0.tasks.stop();
        self.0.tasks.wait();
        result
    }
}
impl fmt::Debug for ConnectionDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionDriver")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}
impl Drop for ConnectionDriver {
    fn drop(&mut self) {
        self.0.stop();
    }
}

impl Connection {
    fn new(
        configuration: Configuration,
        status: ConnectionStatus,
        internal_rpc: InternalRPCHandle,
        events: Events,
    ) -> Self {
        let closer = Arc::new(ConnectionCloser::new(status.clone(), internal_rpc.clone()));
        Self {
            configuration,
            status,
            internal_rpc,
            events,
            io_loop: ThreadHandle::default(),
            closer,
        }
    }

    pub(crate) fn for_reconnect(
        configuration: Configuration,
        status: ConnectionStatus,
        internal_rpc: InternalRPCHandle,
        events: Events,
    ) -> Self {
        let conn = Self::new(configuration, status, internal_rpc, events);
        conn.closer.noop();
        conn.internal_rpc
            .explicit_close
            .store(true, std::sync::atomic::Ordering::Release);
        conn
    }

    /// Connect to an AMQP Server.
    ///
    /// The URI must be in the following format:
    ///
    /// * `amqp://127.0.0.1:5672` will connect to the default virtual host `/`.
    /// * `amqp://127.0.0.1:5672/` will connect to the virtual host `""` (empty string).
    /// * `amqp://127.0.0.1:5672/%2f` will connect to the default virtual host `/`.
    ///
    /// Note that the virtual host has to be escaped with
    /// [URL encoding](https://en.wikipedia.org/wiki/Percent-encoding).
    pub async fn connect(uri: &str, options: ConnectionProperties) -> Result<Self> {
        Connect::connect(uri, options).await
    }

    /// Connect to an AMQP server with an explicit runtime.
    ///
    /// Use this instead of [`connect`] when you need to supply a specific
    /// `async_rs::Runtime` instance rather than the thread-local default.
    ///
    /// [`connect`]: Self::connect
    pub async fn connect_with_runtime<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        uri: &str,
        options: ConnectionProperties,
        runtime: Runtime<RK>,
    ) -> Result<Self> {
        uri.connect_with_config(options, OwnedTLSConfig::default(), runtime)
            .await
    }

    /// Connect to an AMQP server with an explicit runtime and TLS configuration.
    ///
    /// The most flexible entry point: you control both the runtime and the TLS
    /// settings. Use [`connect`] for the common case.
    ///
    /// [`connect`]: Self::connect
    pub async fn connect_with_config<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        uri: &str,
        options: ConnectionProperties,
        config: OwnedTLSConfig,
        runtime: Runtime<RK>,
    ) -> Result<Self> {
        uri.connect_with_config(options, config, runtime).await
    }

    /// Connect to an AMQP server using a pre-parsed [`AMQPUri`].
    ///
    /// Equivalent to [`connect`] but accepts an already-parsed URI.
    ///
    /// [`connect`]: Self::connect
    pub async fn connect_uri(uri: AMQPUri, options: ConnectionProperties) -> Result<Self> {
        Connect::connect(uri, options).await
    }

    /// Connect using a pre-parsed [`AMQPUri`] and an explicit runtime.
    ///
    /// Combines [`connect_uri`] and [`connect_with_runtime`].
    ///
    /// [`connect_uri`]: Self::connect_uri
    /// [`connect_with_runtime`]: Self::connect_with_runtime
    pub async fn connect_uri_with_runtime<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        uri: AMQPUri,
        options: ConnectionProperties,
        runtime: Runtime<RK>,
    ) -> Result<Self> {
        uri.connect_with_config(options, OwnedTLSConfig::default(), runtime)
            .await
    }

    /// Connect using a pre-parsed [`AMQPUri`], an explicit runtime, and TLS configuration.
    ///
    /// Combines [`connect_uri`] and [`connect_with_config`].
    ///
    /// [`connect_uri`]: Self::connect_uri
    /// [`connect_with_config`]: Self::connect_with_config
    pub async fn connect_uri_with_config<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        uri: AMQPUri,
        options: ConnectionProperties,
        config: OwnedTLSConfig,
        runtime: Runtime<RK>,
    ) -> Result<Self> {
        uri.connect_with_config(options, config, runtime).await
    }

    /// Open a new [`Channel`] on this connection.
    ///
    /// Channels are lightweight; open one per concurrent logical task. Returns
    /// an error if the connection is not in the [`crate::ConnectionState::Connected`]
    /// state or if the channel limit negotiated with the server has been reached.
    pub async fn create_channel(&self) -> Result<Channel> {
        self.status.ensure_connected()?;
        self.internal_rpc.create_channel(self.closer.clone()).await
    }

    /// Return a [`Stream`] of connection-level [`Event`]s.
    ///
    /// Events include connection establishment, broker-initiated flow control,
    /// and errors. Clone the stream or call this multiple times to fan-out to
    /// several listeners.
    pub fn events_listener(&self) -> impl Stream<Item = Event> + Send + 'static {
        self.events.listener()
    }

    /// Block the current thread until the connection is closed.
    ///
    /// Useful in simple consumer programs where no other work keeps the
    /// process alive. Drops the connection handle then waits for the
    /// background IO loop thread to finish.
    pub fn run(self) -> Result<()> {
        let io_loop = self.io_loop.clone();
        drop(self);
        io_loop.wait("io loop")
    }

    /// Return the negotiated connection configuration (frame size, heartbeat, …).
    #[must_use]
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    pub(crate) fn configuration_mut(&mut self) -> &mut Configuration {
        &mut self.configuration
    }

    /// Return a snapshot of the current connection state.
    #[must_use]
    pub fn status(&self) -> &ConnectionStatus {
        &self.status
    }

    /// Perform a graceful AMQP connection close.
    ///
    /// Sends `Connection.Close` to the broker and waits for `Connection.Close-Ok`.
    /// `reply_code` should be `200` and `reply_text` `"OK"` for a normal shutdown.
    /// Returns an error if the connection is not in [`crate::ConnectionState::Connected`].
    pub async fn close(&self, reply_code: ReplyCode, reply_text: ShortString) -> Result<()> {
        self.status.ensure_connected()?;
        self.internal_rpc
            .close_connection_checked(reply_code, reply_text, 0, 0)
            .await
    }

    /// Update the authentication secret (e.g. rotate an OAuth2 token).
    ///
    /// Sends `Connection.UpdateSecret` to the broker. `new_secret` is the
    /// replacement token; `reason` is a human-readable explanation logged by
    /// the broker. Use [`auth::TokenAuthProvider`] for automatic rotation.
    ///
    /// [`auth::TokenAuthProvider`]: crate::auth::TokenAuthProvider
    pub async fn update_secret(&self, new_secret: LongString, reason: ShortString) -> Result<()> {
        self.status.ensure_connected()?;
        self.internal_rpc.update_secret(new_secret, reason).await
    }

    /// Low-level entry point for custom transport implementations.
    ///
    /// Drives the AMQP handshake over a transport supplied by the `connect`
    /// closure. Prefer one of the higher-level `connect*` methods unless you
    /// are wrapping a non-standard socket type.
    pub async fn connector<
        RK: RuntimeKit + Clone + Send + 'static,
        S: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static,
    >(
        uri: AMQPUri,
        runtime: Runtime<RK>,
        connect: impl AsyncFn(AMQPUri, Runtime<RK>) -> Result<S> + Send + Sync + 'static,
        options: ConnectionProperties,
    ) -> Result<Self> {
        let (conn, channel0, _control) = Self::connector_parts(uri, runtime, connect, options)?;
        conn.start(channel0).await
    }

    fn connector_parts<
        RK: RuntimeKit + Clone + Send + 'static,
        S: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static,
    >(
        uri: AMQPUri,
        runtime: Runtime<RK>,
        connect: impl AsyncFn(AMQPUri, Runtime<RK>) -> Result<S> + Send + Sync + 'static,
        options: ConnectionProperties,
    ) -> Result<(Self, Channel, ConnectorControl)> {
        let configuration = Configuration::new(&uri, options);
        let status = ConnectionStatus::new(&uri);
        let frames = Frames::default();
        let socket_state = SocketState::default();
        let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
        let secret_update = SecretUpdate::new(
            status.clone(),
            runtime.clone(),
            configuration.auth_provider.clone(),
        );
        let internal_rpc = InternalRPC::new(
            runtime.clone(),
            heartbeat.clone(),
            secret_update,
            frames.clone(),
            socket_state.handle(),
        );
        let events = Events::new();
        events.bind(internal_rpc.handle());
        let channels = Channels::new(
            configuration.clone(),
            status.clone(),
            socket_state.handle(),
            internal_rpc.handle(),
            frames.clone(),
            events.clone(),
        );
        let channel0 = channels.channel0();
        let conn = Connection::new(configuration, status, internal_rpc.handle(), events);
        let force_stop = crate::killswitch::KillSwitch::default();
        let control = ConnectorControl {
            force_stop: force_stop.clone(),
            channels: channels.clone(),
            thread: conn.io_loop.clone(),
            waker: socket_state.handle(),
            rpc: internal_rpc.handle(),
            tasks: internal_rpc.handle().task_scope.clone(),
        };
        let io_loop = IoLoop::new(
            conn.status.clone(),
            conn.configuration.negotiated_config.clone(),
            channels.clone(),
            internal_rpc.handle(),
            frames,
            socket_state,
            heartbeat,
            runtime,
            connect,
            uri,
            conn.configuration().backoff,
            force_stop,
        );

        internal_rpc.start(channels);
        conn.io_loop.register(io_loop.start()?);
        Ok((conn, channel0, control))
    }

    /// Use exactly one caller-owned stream and an explicit authentication provider.
    /// The returned owner exists before negotiation is polled, so failed or
    /// canceled handshakes can be stopped and joined as well. The supplied
    /// runtime owns all async tasks. No DNS, TLS, reconnection, implicit
    /// connection close or token refresh is selected by this entry point.
    pub fn from_stream<
        RK: RuntimeKit + Clone + Send + 'static,
        S: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static,
    >(
        uri: AMQPUri,
        runtime: Runtime<RK>,
        stream: S,
        mut options: ConnectionProperties,
    ) -> Result<(ConnectionDriver, impl Future<Output = Result<Self>> + Send)> {
        if options.auto_recover
            || options
                .auth_provider
                .as_ref()
                .is_none_or(|provider| provider.valid_for().is_some())
        {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,"supplied AMQP transport requires explicit authentication without automatic recovery or refresh").into());
        }
        options.backoff = backon::ExponentialBuilder::default().with_max_times(0);
        let stream = std::sync::Mutex::new(Some(stream));
        let connect = async move |_: AMQPUri, _: Runtime<RK>| {
            stream
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "supplied AMQP transport was already consumed",
                    )
                    .into()
                })
        };
        let (conn, channel0, control) = Self::connector_parts(uri, runtime, connect, options)?;
        conn.closer.noop();
        conn.internal_rpc
            .explicit_close
            .store(true, std::sync::atomic::Ordering::Release);
        Ok((ConnectionDriver(control), conn.start(channel0)))
    }

    /// Start a caller-owned connector under declared retry and recovery policy.
    /// Every connection attempt invokes the supplied closure. No built-in DNS/TLS
    /// is used. An explicit provider is required and automatic token refresh is forbidden.
    pub fn from_connector<
        RK: RuntimeKit + Clone + Send + 'static,
        S: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin + Send + 'static,
    >(
        uri: AMQPUri,
        runtime: Runtime<RK>,
        connect: impl AsyncFn(AMQPUri, Runtime<RK>) -> Result<S> + Send + Sync + 'static,
        options: ConnectionProperties,
    ) -> Result<(ConnectionDriver, impl Future<Output = Result<Self>> + Send)> {
        if options
            .auth_provider
            .as_ref()
            .is_none_or(|p| p.valid_for().is_some())
        {
            return Err(std::io::Error::other(
                "supplied connector requires explicit authentication without token refresh",
            )
            .into());
        }
        let (conn, channel0, control) = Self::connector_parts(uri, runtime, connect, options)?;
        conn.closer.noop();
        conn.internal_rpc
            .explicit_close
            .store(true, std::sync::atomic::Ordering::Release);
        Ok((ConnectionDriver(control), conn.start(channel0)))
    }

    pub(crate) async fn start(self, channel0: Channel) -> Result<Self> {
        let (promise, resolver) = Promise::new("ProtocolHeader");

        trace!("Set connection as connecting");
        self.status.clone().set_connecting()?;

        trace!("Sending protocol header to server");
        channel0.send_frame(
            AMQPFrame::ProtocolHeader(ProtocolVersion::amqp_0_9_1()),
            Box::new(resolver.clone()),
            Some(ExpectedReply(
                Reply::ConnectionStep(ConnectionStep::ProtocolHeader(resolver.clone(), self)),
                Box::new(resolver),
            )),
            None,
        );

        trace!("Sent protocol header to server, waiting for connection flow");
        promise.await
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("configuration", &self.configuration)
            .field("status", &self.status)
            .finish()
    }
}

/// Extension trait that lets URI types open a [`Connection`] directly.
///
/// Implemented for [`&str`], [`String`], and [`AMQPUri`].
#[async_trait]
pub trait Connect {
    /// Connect to an AMQP server using the default runtime and TLS configuration.
    async fn connect(self, options: ConnectionProperties) -> Result<Connection>
    where
        Self: Sized,
    {
        self.connect_with_config(
            options,
            OwnedTLSConfig::default(),
            runtime::default_runtime()?,
        )
        .await
    }

    /// Connect to an AMQP server with an explicit runtime and TLS configuration.
    async fn connect_with_config<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        self,
        options: ConnectionProperties,
        config: OwnedTLSConfig,
        runtime: Runtime<RK>,
    ) -> Result<Connection>
    where
        Self: Sized;
}

#[async_trait]
impl Connect for AMQPUri {
    async fn connect_with_config<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        self,
        options: ConnectionProperties,
        config: OwnedTLSConfig,
        runtime: Runtime<RK>,
    ) -> Result<Connection> {
        Connection::connector(
            self,
            runtime,
            async move |uri, runtime| {
                AMQPUriTcpExt::connect_with_config_async(&uri, config.as_ref(), &runtime)
                    .await
                    .map_err(|err| Error::io(err, &runtime))
            },
            options,
        )
        .await
    }
}

#[async_trait]
impl Connect for &str {
    async fn connect_with_config<RK: RuntimeKit + Send + Sync + Clone + 'static>(
        self,
        options: ConnectionProperties,
        config: OwnedTLSConfig,
        runtime: Runtime<RK>,
    ) -> Result<Connection> {
        match self.parse::<AMQPUri>() {
            Ok(uri) => uri.connect_with_config(options, config, runtime).await,
            Err(err) => Err(Error::other(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BasicProperties, ChannelState, ConnectionProperties, ConnectionState, ErrorKind,
        channel_receiver_state::{ChannelReceiverState, DeliveryCause},
        options::BasicConsumeOptions,
        secret_update::SecretUpdate,
        types::{ChannelId, FieldTable, ShortString},
    };
    use amq_protocol::{
        frame::AMQPContentHeader,
        protocol::{AMQPClass, basic},
    };

    fn create_connection() -> (Connection, Channels, InternalRPCHandle) {
        let (conn, channels, internal_rpc, _) = create_connection_with_frames();
        (conn, channels, internal_rpc)
    }

    fn create_connection_with_frames() -> (Connection, Channels, InternalRPCHandle, Frames) {
        let uri = AMQPUri::default();
        let runtime = runtime::default_runtime().unwrap();
        let configuration = Configuration::new(&uri, ConnectionProperties::default());
        let status = ConnectionStatus::new(&uri);
        let frames = Frames::default();
        let socket_state = SocketState::default();
        let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
        let secret_update = SecretUpdate::new(
            status.clone(),
            runtime.clone(),
            configuration.auth_provider.clone(),
        );
        let internal_rpc = InternalRPC::new(
            runtime,
            heartbeat,
            secret_update,
            frames.clone(),
            socket_state.handle(),
        );
        let events = Events::new();
        events.bind(internal_rpc.handle());
        let channels = Channels::new(
            configuration.clone(),
            status.clone(),
            socket_state.handle(),
            internal_rpc.handle(),
            frames.clone(),
            events.clone(),
        );
        let conn = Connection::new(configuration, status, internal_rpc.handle(), events);
        conn.status.set_state(ConnectionState::Connected);
        (conn, channels, internal_rpc.handle(), frames)
    }

    #[test]
    fn clear_connection_steps_rejects_all_handshake_stages() {
        let frames = Frames::default();
        let error: Error = ErrorKind::MissingHeartbeatError.into();
        let mut promises = Vec::new();
        for stage in 0..3 {
            let (conn, _, _) = create_connection();
            let (promise, resolver) = Promise::new("handshake cleanup");
            let auth_provider = conn.configuration.auth_provider.clone();
            let step = match stage {
                0 => ConnectionStep::ProtocolHeader(resolver.clone(), conn),
                1 => ConnectionStep::StartOk(resolver.clone(), conn, auth_provider),
                _ => ConnectionStep::SecureOk(resolver.clone(), conn, auth_provider),
            };
            frames.push(
                0,
                AMQPFrame::ProtocolHeader(ProtocolVersion::amqp_0_9_1()),
                Box::new(resolver.clone()),
                Some(ExpectedReply(
                    Reply::ConnectionStep(step),
                    Box::new(resolver),
                )),
                None,
            );
            promises.push(promise);
        }
        let (other_promise, other_resolver) = Promise::new("unrelated reply");
        frames.push(
            0,
            AMQPFrame::Heartbeat,
            Box::new(other_resolver.clone()),
            Some(ExpectedReply(
                Reply::BasicCancelOk(other_resolver.clone()),
                Box::new(other_resolver),
            )),
            None,
        );

        frames.clear_connection_steps(&error);
        for promise in promises {
            assert!(matches!(
                promise.try_wait().unwrap().unwrap_err().kind(),
                ErrorKind::MissingHeartbeatError
            ));
        }
        assert!(frames.connection_resolver(0).is_none());
        assert!(other_promise.try_wait().is_none());
        assert_eq!(frames.take_expected_replies(0).unwrap().len(), 1);
        // Clearing an empty queue is safe, including repeated cleanup.
        frames.clear_connection_steps(&error);
        frames.clear_connection_steps(&error);
    }

    #[test]
    fn retry_connection_header_preserves_waiter_and_replaces_queued_header() {
        use std::{future::Future, task::Context};

        let (conn, channels, _, frames) = create_connection_with_frames();
        let mut connecting = Box::pin(conn.start(channels.channel0()));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(connecting.as_mut().poll(&mut cx).is_pending());

        // Retrying before the header is sent must not duplicate it, even when
        // the previous attempt already put it in the retry queue.
        assert!(frames.retry_connection_header());
        assert!(frames.retry_connection_header());
        assert!(matches!(
            *frames.pop(true).unwrap(),
            AMQPFrame::ProtocolHeader(_)
        ));
        assert!(frames.pop(true).is_none());
        assert!(connecting.as_mut().poll(&mut cx).is_pending());

        frames.clear_connection_steps(&ErrorKind::MissingHeartbeatError.into());
        assert!(matches!(
            connecting.as_mut().poll(&mut cx),
            std::task::Poll::Ready(Err(_))
        ));
        assert!(!frames.retry_connection_header());
    }

    #[test]
    fn recovery_rejects_pending_connection_handshake() {
        use std::{
            future::Future,
            sync::atomic::{AtomicBool, Ordering},
            task::{Context, Wake, Waker},
        };

        struct WakeFlag(AtomicBool);
        impl Wake for WakeFlag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let (conn, channels, _, frames) = create_connection_with_frames();
        let status = conn.status.clone();
        let mut connecting = Box::pin(conn.start(channels.channel0()));
        let wake = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(connecting.as_mut().poll(&mut cx).is_pending());
        // The header has left the outbound queue: only the expected reply can
        // reject the handshake now, not drop_frames_for_channel during recovery.
        drop(frames.pop(true).unwrap());

        let error: Error = ErrorKind::MissingHeartbeatError.into();
        channels.init_connection_recovery(error.clone());

        assert!(wake.0.load(Ordering::SeqCst));
        assert!(status.reconnecting());
        match connecting.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(Err(actual)) => {
                assert!(matches!(actual.kind(), ErrorKind::MissingHeartbeatError));
            }
            other => panic!("expected the handshake to fail, got {other:?}"),
        }
        assert!(frames.connection_resolver(0).is_none());
    }

    #[test]
    fn channel_limit() {
        let _ = tracing_subscriber::fmt::try_init();

        // Bootstrap connection state to a consuming state
        let (conn, channels, _) = create_connection();
        conn.configuration
            .negotiated_config
            .set_channel_max(ChannelId::MAX);
        for _ in 1..=ChannelId::MAX {
            channels.create(conn.closer.clone()).unwrap();
        }

        assert_eq!(
            channels.create(conn.closer.clone()),
            Err(ErrorKind::ChannelsLimitReached.into())
        );
    }

    #[test]
    fn basic_consume_small_payload() {
        let _ = tracing_subscriber::fmt::try_init();

        use crate::consumer::Consumer;

        // Bootstrap connection state to a consuming state
        let (conn, channels, internal_rpc) = create_connection();
        conn.configuration.negotiated_config.set_channel_max(2047);
        let channel = channels.create(conn.closer.clone()).unwrap();
        channel.set_state(ChannelState::Connected);
        let queue_name = ShortString::from("consumed");
        let consumer_tag = ShortString::from("consumer-tag");
        let consumer = Consumer::new(
            consumer_tag.clone(),
            internal_rpc,
            None,
            queue_name.clone(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        );
        if let Some(c) = channels.get(channel.id()) {
            c.register_consumer(consumer_tag.clone(), consumer);
            c.register_queue(queue_name.clone(), Default::default(), Default::default());
        }
        // Now test the state machine behaviour
        {
            let method = AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                consumer_tag: consumer_tag.clone(),
                delivery_tag: 1,
                redelivered: false,
                exchange: "".into(),
                routing_key: queue_name,
            }));
            let class_id = method.get_amqp_class_id();
            let deliver_frame = AMQPFrame::Method(channel.id(), method);
            channels.handle_frame(deliver_frame).unwrap();
            let channel_state = channel.status().receiver_state();
            let expected_state = ChannelReceiverState::WillReceiveContent(
                class_id,
                DeliveryCause::Consume(consumer_tag.clone()),
            );
            assert_eq!(channel_state, expected_state);
        }
        {
            let header_frame = AMQPFrame::Header(
                channel.id(),
                AMQPContentHeader {
                    class_id: 60,
                    body_size: 2,
                    properties: BasicProperties::default(),
                },
            );
            channels.handle_frame(header_frame).unwrap();
            let channel_state = channel.status().receiver_state();
            let expected_state =
                ChannelReceiverState::ReceivingContent(DeliveryCause::Consume(consumer_tag), 2);
            assert_eq!(channel_state, expected_state);
        }
        {
            let body_frame = AMQPFrame::Body(channel.id(), b"{}".to_vec());
            channels.handle_frame(body_frame).unwrap();
            assert!(channel.status().connected());
        }
    }

    #[test]
    fn basic_consume_empty_payload() {
        let _ = tracing_subscriber::fmt::try_init();

        use crate::consumer::Consumer;

        // Bootstrap connection state to a consuming state
        let (conn, channels, internal_rpc) = create_connection();
        conn.configuration.negotiated_config.set_channel_max(2047);
        let channel = channels.create(conn.closer.clone()).unwrap();
        channel.set_state(ChannelState::Connected);
        let queue_name = ShortString::from("consumed");
        let consumer_tag = ShortString::from("consumer-tag");
        let consumer = Consumer::new(
            consumer_tag.clone(),
            internal_rpc,
            None,
            queue_name.clone(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        );
        if let Some(c) = channels.get(channel.id()) {
            c.register_consumer(consumer_tag.clone(), consumer);
            c.register_queue(queue_name.clone(), Default::default(), Default::default());
        }
        // Now test the state machine behaviour
        {
            let method = AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                consumer_tag: consumer_tag.clone(),
                delivery_tag: 1,
                redelivered: false,
                exchange: "".into(),
                routing_key: queue_name,
            }));
            let class_id = method.get_amqp_class_id();
            let deliver_frame = AMQPFrame::Method(channel.id(), method);
            channels.handle_frame(deliver_frame).unwrap();
            let channel_state = channel.status().receiver_state();
            let expected_state = ChannelReceiverState::WillReceiveContent(
                class_id,
                DeliveryCause::Consume(consumer_tag),
            );
            assert_eq!(channel_state, expected_state);
        }
        {
            let header_frame = AMQPFrame::Header(
                channel.id(),
                AMQPContentHeader {
                    class_id: 60,
                    body_size: 0,
                    properties: BasicProperties::default(),
                },
            );
            channels.handle_frame(header_frame).unwrap();
            assert!(channel.status().connected());
        }
    }
    #[test]
    fn stale_generation_cannot_enqueue_an_acknowledgement() {
        let (connection, channels, _, frames) = create_connection_with_frames();
        connection.closer.noop();
        connection
            .configuration
            .negotiated_config
            .set_channel_max(4);
        let channel = channels.create(connection.closer.clone()).unwrap();
        channel.set_state(ChannelState::Connected);
        let result = futures_lite::future::block_on(
            channel
                .at_generation(1)
                .basic_ack(1, crate::options::BasicAckOptions::default()),
        );
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::StaleGeneration
        ));
        assert!(!frames.has_pending());
    }
}
