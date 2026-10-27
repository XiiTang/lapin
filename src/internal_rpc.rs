use crate::{
    Channel, Error, ErrorKind, Promise, PromiseResolver, Result,
    channels::Channels,
    connection_closer::ConnectionCloser,
    connection_step::HandshakeResolver,
    consumer_status::ConsumerStatus,
    error_holder::ErrorHolder,
    frames::Frames,
    future::InternalFuture,
    heartbeat::Heartbeat,
    killswitch::KillSwitch,
    options::{BasicAckOptions, BasicCancelOptions, BasicNackOptions, BasicRejectOptions},
    secret_update::SecretUpdate,
    socket_state::SocketStateHandle,
    types::{ChannelId, DeliveryTag, Identifier, LongString, ReplyCode, ShortString},
};
use amq_protocol::frame::AMQPFrame;
use async_rs::{Runtime, traits::*};
use flume::{Receiver, Sender};
use std::{collections::HashMap, fmt, future::Future, sync::Arc, time::Duration};
use tracing::{debug, trace};

const INTERNAL_RPC_QUEUE_MESSAGES: usize = 256;
// There is no lapin close/handshake deadline setting. Heartbeats may be zero.
// A rejected frame gets a finite best-effort write, never an unbounded RPC.
const REJECTION_CLOSE_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct InternalRPC<RK: RuntimeKit + Clone + Send + 'static> {
    rpc: Receiver<Option<InternalCommand>>,
    handle: InternalRPCHandle,
    channels_status: HashMap<ChannelId, KillSwitch>,
    frames: Frames,
    heartbeat: Heartbeat<RK>,
    secret_update: SecretUpdate<RK>,
    runtime: Runtime<RK>,
}

#[derive(Clone)]
pub(crate) struct InternalRPCHandle {
    sender: Sender<Option<InternalCommand>>,
    waker: SocketStateHandle,
    pub(crate) task_scope: crate::task_scope::TaskScope,
    pub(crate) explicit_close: Arc<std::sync::atomic::AtomicBool>,
    overflow: Arc<std::sync::atomic::AtomicBool>,
    connection_status: crate::ConnectionStatus,
    #[cfg(test)]
    close_command_gap: Arc<std::sync::Mutex<Option<tests::CloseCommandGap>>>,
    #[cfg(test)]
    reply_poll_gap: Arc<std::sync::Mutex<Option<tests::ReplyPollGap>>>,
}

impl InternalRPCHandle {
    // Every public RPC-queue caller waits here. Keep completed successes and
    // errors; resolve proven abandonment through the shutdown owner.
    pub(crate) async fn await_reply<T>(&self, mut promise: Promise<T>) -> Result<T> {
        use crate::promise::Completion;
        use std::task::Poll;

        let mut retired = std::pin::pin!(self.task_scope.retired());
        std::future::poll_fn(|cx| {
            let completion = match promise.poll_completion(cx) {
                Poll::Ready(completion) => Some(completion),
                Poll::Pending => {
                    #[cfg(test)]
                    tests::after_pending_reply_poll(self);
                    if retired.as_mut().poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    // The last producer can complete between the first poll
                    // and releasing its guard. Retirement synchronizes with
                    // that release; recheck the final reply before cancelling.
                    match promise.poll_completion(cx) {
                        Poll::Ready(completion) => Some(completion),
                        Poll::Pending => None,
                    }
                }
            };
            let error = match completion {
                Some(Completion::Completed(result)) => return Poll::Ready(result),
                Some(Completion::Abandoned(error)) => error,
                None => std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "RPC scope retired before reply completion",
                )
                .into(),
            };
            Poll::Ready(Err(self.connection_status.rejection().unwrap_or(error)))
        })
        .await
    }

    pub(crate) fn basic_ack(
        &self,
        channel_id: ChannelId,
        delivery_tag: DeliveryTag,
        options: BasicAckOptions,
        resolver: PromiseResolver<()>,
        error: Option<ErrorHolder>,
    ) {
        self.send(InternalCommand::BasicAck(
            channel_id,
            delivery_tag,
            options,
            resolver,
            error,
        ));
    }

    pub(crate) fn basic_nack(
        &self,
        channel_id: ChannelId,
        delivery_tag: DeliveryTag,
        options: BasicNackOptions,
        resolver: PromiseResolver<()>,
        error: Option<ErrorHolder>,
    ) {
        self.send(InternalCommand::BasicNack(
            channel_id,
            delivery_tag,
            options,
            resolver,
            error,
        ));
    }

    pub(crate) fn basic_reject(
        &self,
        channel_id: ChannelId,
        delivery_tag: DeliveryTag,
        options: BasicRejectOptions,
        resolver: PromiseResolver<()>,
        error: Option<ErrorHolder>,
    ) {
        self.send(InternalCommand::BasicReject(
            channel_id,
            delivery_tag,
            options,
            resolver,
            error,
        ));
    }

    pub(crate) fn cancel_consumer(
        &self,
        channel_id: ChannelId,
        consumer_tag: ShortString,
        consumer_status: ConsumerStatus,
    ) {
        self.send(InternalCommand::CancelConsumer(
            channel_id,
            consumer_tag,
            consumer_status,
        ));
    }

    pub(crate) fn close_channel(
        &self,
        channel_id: ChannelId,
        reply_code: ReplyCode,
        reply_text: ShortString,
    ) {
        self.send(InternalCommand::CloseChannel(
            channel_id, reply_code, reply_text,
        ));
    }

    pub(crate) fn close_connection(
        &self,
        reply_code: ReplyCode,
        reply_text: ShortString,
        class_id: Identifier,
        method_id: Identifier,
    ) {
        self.set_connection_closing();
        self.send(InternalCommand::CloseConnection(
            reply_code, reply_text, class_id, method_id, None,
        ));
    }

    // All receive paths which return ProtocolViolation enter this handoff.
    pub(crate) fn reject_frame(
        &self,
        channel: ChannelId,
        error: Error,
        class: Identifier,
        method: Identifier,
        handshake: Option<HandshakeResolver>,
    ) {
        self.send(InternalCommand::RejectFrame(
            channel, error, class, method, handshake,
        ));
    }

    pub(crate) async fn close_connection_checked(
        &self,
        reply_code: ReplyCode,
        reply_text: ShortString,
        class_id: Identifier,
        method_id: Identifier,
    ) -> Result<()> {
        self.set_connection_closing();
        #[cfg(test)]
        tests::pause_between_close_commands(self).await;
        let (promise, resolver) = Promise::new("connection.close");
        self.send(InternalCommand::CloseConnection(
            reply_code,
            reply_text,
            class_id,
            method_id,
            Some(resolver),
        ));
        self.await_reply(promise).await
    }

    pub(crate) async fn create_channel(
        &self,
        connection_closer: Arc<ConnectionCloser>,
    ) -> Result<Channel> {
        let (promise, resolver) = Promise::new("channel.create");
        self.send(InternalCommand::CreateChannel(connection_closer, resolver));
        self.await_reply(promise).await
    }

    pub(crate) fn deregister_consumer(&self, channel_id: ChannelId, consumer_tag: ShortString) {
        self.send(InternalCommand::DeregisterConsumer(
            channel_id,
            consumer_tag,
        ));
    }

    pub(crate) fn finish_connection_shutdown(&self) {
        self.send(InternalCommand::FinishConnectionShutdown);
    }

    pub(crate) fn init_connection_recovery(&self, error: Error) {
        self.send(InternalCommand::InitConnectionRecovery(error));
    }

    pub(crate) fn init_connection_shutdown(&self, error: Error) {
        self.set_connection_closing();
        self.send(InternalCommand::InitConnectionShutdown(error));
    }

    pub(crate) fn remove_channel(&self, channel_id: ChannelId, error: Error) {
        self.send(InternalCommand::RemoveChannel(channel_id, error));
    }

    pub(crate) fn send_connection_close_ok(
        &self,
        error: Error,
        handshake: Option<HandshakeResolver>,
    ) {
        self.send(InternalCommand::SendConnectionCloseOk(error, handshake));
    }

    pub(crate) fn send_heartbeat(&self) {
        self.send(InternalCommand::SendHeartbeat);
    }

    pub(crate) fn set_channel_status(&self, channel_id: ChannelId, killswitch: KillSwitch) {
        self.send(InternalCommand::SetChannelStatus(channel_id, killswitch));
    }

    pub(crate) fn set_connection_closing(&self) {
        self.send(InternalCommand::SetConnectionClosing);
    }

    pub(crate) fn set_connection_closed(&self, error: Error) {
        self.send(InternalCommand::SetConnectionClosed(error));
    }

    pub(crate) fn set_connection_error(&self, error: Error) {
        self.send(InternalCommand::SetConnectionError(error));
    }

    pub(crate) fn spawn(&self, f: impl Future<Output = Result<()>> + Send + 'static) {
        self.send(InternalCommand::Spawn(InternalFuture(Box::pin(f))));
    }

    pub(crate) fn spawn_infallible(&self, f: impl Future<Output = ()> + Send + 'static) {
        self.spawn(async move {
            f.await;
            Ok(())
        });
    }

    pub(crate) fn start_channels_recovery(&self) {
        self.send(InternalCommand::StartChannelsRecovery);
    }

    pub(crate) fn start_heartbeat(&self, heartbeat: Duration) {
        self.send(InternalCommand::StartHeartbeat(heartbeat));
    }

    pub(crate) async fn update_secret(
        &self,
        secret: LongString,
        reason: ShortString,
    ) -> Result<()> {
        let (promise, resolver) = Promise::new("connection.update-secret");
        self.send(InternalCommand::UpdateSecret(secret, reason, resolver));
        self.await_reply(promise).await
    }

    pub(crate) fn stop(&self) {
        trace!("Stopping internal RPC command");
        if self.sender.try_send(None).is_err() {
            self.overflow
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    fn send(&self, command: InternalCommand) {
        trace!(?command, "Queuing internal RPC command");
        // The only scenario where this can fail if this is the IoLoop already exited
        if self.sender.try_send(Some(command)).is_err() {
            self.overflow
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.waker.wake();
    }
}

impl fmt::Debug for InternalRPCHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InternalRPCHandle").finish()
    }
}

#[derive(Debug)]
enum InternalCommand {
    BasicAck(
        ChannelId,
        DeliveryTag,
        BasicAckOptions,
        PromiseResolver<()>,
        Option<ErrorHolder>,
    ),
    BasicNack(
        ChannelId,
        DeliveryTag,
        BasicNackOptions,
        PromiseResolver<()>,
        Option<ErrorHolder>,
    ),
    BasicReject(
        ChannelId,
        DeliveryTag,
        BasicRejectOptions,
        PromiseResolver<()>,
        Option<ErrorHolder>,
    ),
    CancelConsumer(ChannelId, ShortString, ConsumerStatus),
    CloseChannel(ChannelId, ReplyCode, ShortString),
    CloseConnection(
        ReplyCode,
        ShortString,
        Identifier,
        Identifier,
        Option<PromiseResolver<()>>,
    ),
    CreateChannel(Arc<ConnectionCloser>, PromiseResolver<Channel>),
    DeregisterConsumer(ChannelId, ShortString),
    FinishConnectionShutdown,
    FinishFrameRejection(Error),
    RejectFrame(
        ChannelId,
        Error,
        Identifier,
        Identifier,
        Option<HandshakeResolver>,
    ),
    InitConnectionRecovery(Error),
    InitConnectionShutdown(Error),
    RemoveChannel(ChannelId, Error),
    SendConnectionCloseOk(Error, Option<HandshakeResolver>),
    SendHeartbeat,
    SetChannelStatus(ChannelId, KillSwitch),
    SetConnectionClosing,
    SetConnectionClosed(Error),
    SetConnectionError(Error),
    Spawn(InternalFuture),
    StartChannelsRecovery,
    StartHeartbeat(Duration),
    UpdateSecret(LongString, ShortString, PromiseResolver<()>),
}

impl<RK: RuntimeKit + Clone + Send + 'static> InternalRPC<RK> {
    pub(crate) fn new(
        runtime: Runtime<RK>,
        heartbeat: Heartbeat<RK>,
        secret_update: SecretUpdate<RK>,
        frames: Frames,
        waker: SocketStateHandle,
        connection_status: crate::ConnectionStatus,
    ) -> Self {
        let (sender, rpc) = flume::bounded(INTERNAL_RPC_QUEUE_MESSAGES);
        let handle = InternalRPCHandle {
            connection_status,
            sender,
            waker,
            task_scope: Default::default(),
            explicit_close: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            overflow: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            close_command_gap: Default::default(),
            #[cfg(test)]
            reply_poll_gap: Default::default(),
        };
        Self {
            rpc,
            handle,
            channels_status: Default::default(),
            frames,
            heartbeat,
            secret_update,
            runtime,
        }
    }

    pub(crate) fn handle(&self) -> InternalRPCHandle {
        self.handle.clone()
    }

    fn channel_ok(&self, chan: ChannelId) -> bool {
        self.channels_status
            .get(&chan)
            .is_some_and(|killswitch| !killswitch.killed())
    }

    pub(crate) fn register_internal_future(
        &self,
        fut: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        let handle = self.handle();
        self.runtime.spawn(self.handle.task_scope.wrap(async move {
            if let Err(err) = fut.await {
                handle.set_connection_error(err);
            }
        }));
    }

    pub(crate) fn register_internal_future_with_resolver<T: Send + 'static>(
        &self,
        fut: impl Future<Output = Result<T>> + Send + 'static,
        resolver: PromiseResolver<T>,
    ) {
        self.register_internal_future(async move {
            let res = fut.await;
            resolver.complete(res);
            Ok(())
        });
    }

    fn apply_connection_error(&self, channels: &Channels, error: Error) {
        if channels.can_recover(&error) {
            if channels.should_init_connection_recovery() {
                self.handle.init_connection_recovery(error);
            }
        } else {
            channels.set_connection_error(error)
        }
    }

    pub(crate) fn start(self, channels: Channels) {
        self.runtime
            .clone()
            .spawn(self.handle.task_scope.clone().wrap(self.run(channels)));
    }

    async fn run(mut self, channels: Channels) {
        use InternalCommand::*;

        let rpc = self.rpc.clone();
        let get_channel = |id| {
            channels
                .get(id)
                .ok_or::<Error>(ErrorKind::InvalidChannel(id).into())
        };

        while let Ok(Some(command)) = rpc.recv_async().await {
            if self
                .handle
                .overflow
                .load(std::sync::atomic::Ordering::Acquire)
            {
                channels.set_connection_error(
                    crate::ResourceLimit::InternalRpcQueueMessages.exceeded(
                        INTERNAL_RPC_QUEUE_MESSAGES,
                        Some(INTERNAL_RPC_QUEUE_MESSAGES as u64 + 1),
                    ),
                );
                break;
            }
            trace!(?command, "Handling internal RPC command");
            match command {
                BasicAck(channel_id, delivery_tag, options, resolver, error) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channel = get_channel(channel_id);
                    self.register_internal_future_with_resolver(
                        async move {
                            if let Some(error) = error {
                                error.check()?;
                            }
                            channel?.basic_ack(delivery_tag, options).await
                        },
                        resolver,
                    )
                }
                BasicNack(channel_id, delivery_tag, options, resolver, error) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channel = get_channel(channel_id);
                    self.register_internal_future_with_resolver(
                        async move {
                            if let Some(error) = error {
                                error.check()?;
                            }
                            channel?.basic_nack(delivery_tag, options).await
                        },
                        resolver,
                    )
                }
                BasicReject(channel_id, delivery_tag, options, resolver, error) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channel = get_channel(channel_id);
                    self.register_internal_future_with_resolver(
                        async move {
                            if let Some(error) = error {
                                error.check()?;
                            }
                            channel?.basic_reject(delivery_tag, options).await
                        },
                        resolver,
                    )
                }
                CancelConsumer(channel_id, consumer_tag, consumer_status) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channel = get_channel(channel_id);
                    self.register_internal_future(async move {
                        let channel = channel?;
                        if channel.status().connected() && consumer_status.state().is_active() {
                            channel
                                .basic_cancel(consumer_tag, BasicCancelOptions::default())
                                .await
                        } else {
                            Ok(())
                        }
                    })
                }
                CloseChannel(channel_id, reply_code, reply_text) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channel = get_channel(channel_id);
                    self.register_internal_future(async move {
                        channel?.close(reply_code, reply_text).await
                    })
                }
                CloseConnection(reply_code, reply_text, class_id, method_id, resolver) => {
                    if let Some(error) = self.frames.poison() {
                        if let Some(resolver) = resolver {
                            resolver.reject(error);
                        }
                        continue;
                    }
                    let channel0 = channels.channel0();
                    let fut = async move {
                        channel0
                            .connection_close(reply_code, reply_text, class_id, method_id)
                            .await
                    };
                    if let Some(resolver) = resolver {
                        self.register_internal_future_with_resolver(fut, resolver)
                    } else {
                        self.register_internal_future(fut)
                    }
                }
                RejectFrame(channel_id, error, class_id, method_id, handshake) => {
                    let deadline = self.runtime.sleep(REJECTION_CLOSE_WRITE_TIMEOUT);
                    self.heartbeat.cancel();
                    self.secret_update.cancel();
                    channels.begin_frame_rejection();
                    let written = get_channel(channel_id).map(|channel| {
                        channel.protocol_violation_close(&error, class_id, method_id)
                    });
                    let handle = self.handle();
                    self.register_internal_future(async move {
                        if let Ok(written) = written {
                            // A failed write retires with the original verdict too.
                            let mut written = std::pin::pin!(written);
                            let mut deadline = std::pin::pin!(deadline);
                            std::future::poll_fn(|cx| {
                                if written.as_mut().poll(cx).is_ready()
                                    || deadline.as_mut().poll(cx).is_ready()
                                {
                                    std::task::Poll::Ready(())
                                } else {
                                    std::task::Poll::Pending
                                }
                            })
                            .await;
                        }
                        if let Some(handshake) = handshake {
                            handshake.finish_close();
                        }
                        handle.send(FinishFrameRejection(error));
                        Ok(())
                    });
                }
                FinishFrameRejection(error) => {
                    channels.set_connection_error(error);
                }
                CreateChannel(closer, resolver) => match channels.create(closer) {
                    Ok(channel) => self.register_internal_future_with_resolver(
                        async move { channel.clone().channel_open(channel).await },
                        resolver,
                    ),
                    Err(err) => resolver.reject(err),
                },
                DeregisterConsumer(channel_id, consumer_tag) => {
                    if let Ok(channel) = get_channel(channel_id) {
                        channel.deregister_consumer(consumer_tag.as_str());
                    }
                }
                FinishConnectionShutdown => channels.finish_connection_shutdown(),
                InitConnectionRecovery(error) => {
                    self.heartbeat.reset();
                    self.secret_update.reset();
                    channels.init_connection_recovery(error);
                }
                InitConnectionShutdown(error) => channels.init_connection_shutdown(error),
                RemoveChannel(channel_id, error) => {
                    if !self.channel_ok(channel_id) {
                        continue;
                    }
                    let channels = channels.clone();
                    self.register_internal_future(async move { channels.remove(channel_id, error) })
                }
                SendConnectionCloseOk(error, handshake) => {
                    let channel = channels.channel0();
                    self.register_internal_future(async move {
                        // Retire only after the frame-write completion. Merely
                        // queueing CloseOk must not stop I/O or discard the frame.
                        let sent = channel.connection_close_ok().await;
                        // A failed handshake releases its caller-owned driver.
                        // Keep its original cause, but not ahead of CloseOk.
                        if let Some(handshake) = handshake {
                            handshake.finish_close();
                        }
                        sent?;
                        channel.on_connection_close_ok_sent(error);
                        Ok(())
                    })
                }
                SendHeartbeat => {
                    debug!("send heartbeat");
                    let (promise, resolver) = Promise::new("Heartbeat");
                    channels.channel0().send_frame(
                        AMQPFrame::Heartbeat,
                        Box::new(resolver.clone()),
                        None,
                        Some(resolver),
                    );
                    self.register_internal_future(promise);
                }
                SetChannelStatus(channel_id, killswitch) => {
                    self.channels_status.insert(channel_id, killswitch);
                }
                SetConnectionClosing => {
                    self.heartbeat.cancel();
                    self.secret_update.cancel();
                    channels.set_connection_closing();
                }
                SetConnectionClosed(error) => {
                    channels.set_connection_closed(error);
                }
                SetConnectionError(error) => {
                    self.apply_connection_error(&channels, error);
                }
                Spawn(fut) => self.register_internal_future(fut),
                StartChannelsRecovery => {
                    let channels = channels.clone();
                    self.register_internal_future(async move { channels.start_recovery().await })
                }
                StartHeartbeat(heartbeat) => {
                    self.heartbeat.set_timeout(heartbeat);
                    self.heartbeat.start(self.handle());
                    self.secret_update.start(self.handle());
                }
                UpdateSecret(secret, reason, resolver) => {
                    let channels = channels.clone();
                    self.register_internal_future_with_resolver(
                        async move {
                            channels
                                .channel0()
                                .connection_update_secret(secret, reason)
                                .await
                        },
                        resolver,
                    )
                }
            }
            self.handle.waker.wake();
        }
        trace!("InternalRPC stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    pub(super) struct CloseCommandGap {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    pub(super) async fn pause_between_close_commands(handle: &InternalRPCHandle) {
        let gap = handle.close_command_gap.lock().unwrap().take();
        if let Some(gap) = gap {
            gap.entered.notify_one();
            gap.release.notified().await;
        }
    }

    #[tokio::test]
    async fn local_close_between_commands_keeps_the_reserved_rejection_after_scope_retirement() {
        let runtime = Runtime::tokio_current();
        let uri = crate::uri::AMQPUri::default();
        let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
        let status = crate::ConnectionStatus::new(&uri);
        let socket = crate::socket_state::SocketState::default();
        let frames = Frames::default();
        let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
        let secret_update = SecretUpdate::new(
            status.clone(),
            runtime.clone(),
            config.auth_provider.clone(),
        );
        let rpc = InternalRPC::new(
            runtime,
            heartbeat,
            secret_update,
            frames.clone(),
            socket.handle(),
            status.clone(),
        );
        let handle = rpc.handle();
        let events = crate::events::Events::new();
        let channels = Channels::new(
            config,
            status.clone(),
            socket.handle(),
            handle.clone(),
            frames.clone(),
            events,
        );
        let error: Error = ErrorKind::ProtocolViolation(crate::protocol::AMQPError::new(
            crate::protocol::AMQPHardError::COMMANDINVALID.into(),
            "rejected frame".into(),
        ))
        .into();
        assert!(status.admit_rejection(error.clone()));
        handle.reject_frame(0, error, 10, 50, None);
        let mut run = Box::pin(handle.task_scope.wrap(rpc.run(channels)));
        assert!(
            futures_lite::future::poll_once(run.as_mut())
                .await
                .is_none()
        );
        let close = frames.pop(false).expect("the rejection's close is pending");
        assert!(
            matches!(&*close, AMQPFrame::Method(0, crate::protocol::AMQPClass::Connection(crate::protocol::connection::AMQPMethod::Close(close))) if close.reply_code == 503)
        );
        let (_size, _held_write) = close.into_serialized_frame(1);

        let gap = CloseCommandGap::default();
        *handle.close_command_gap.lock().unwrap() = Some(gap.clone());
        let mut caller = Box::pin(handle.close_connection_checked(200, "local close".into(), 0, 0));
        assert!(
            futures_lite::future::poll_once(caller.as_mut())
                .await
                .is_none()
        );
        gap.entered.notified().await;
        // The first command abandons the held write with the admitted verdict.
        assert!(
            futures_lite::future::poll_once(run.as_mut())
                .await
                .is_none()
        );
        assert!(status.errored());
        assert!(
            matches!(frames.poison().unwrap().kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503)
        );
        // Finish the same scoped retirement that drops queued commands and
        // in-flight RPC tasks. The caller has not created its resolver yet.
        handle.task_scope.stop();
        handle.stop();
        run.await;
        let scope = handle.task_scope.clone();
        tokio::task::spawn_blocking(move || scope.wait())
            .await
            .unwrap();
        assert!(handle.task_scope.is_finished());
        gap.release.notify_one();
        let result = caller.await.unwrap_err();
        assert!(
            matches!(result.kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn queued_rpc_callers_keep_the_reserved_rejection_when_the_receiver_retires() {
        for operation in 0..6 {
            let runtime = Runtime::tokio_current();
            let uri = crate::uri::AMQPUri::default();
            let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
            let status = crate::ConnectionStatus::new(&uri);
            let socket = crate::socket_state::SocketState::default();
            let frames = Frames::default();
            let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
            let secret_update = SecretUpdate::new(
                status.clone(),
                runtime.clone(),
                config.auth_provider.clone(),
            );
            let rpc = InternalRPC::new(
                runtime,
                heartbeat,
                secret_update,
                frames.clone(),
                socket.handle(),
                status.clone(),
            );
            let handle = rpc.handle();
            let channels = Channels::new(
                config,
                status.clone(),
                socket.handle(),
                handle.clone(),
                frames,
                crate::events::Events::new(),
            );
            let error: Error = ErrorKind::ProtocolViolation(crate::protocol::AMQPError::new(
                crate::protocol::AMQPHardError::COMMANDINVALID.into(),
                "rejected frame".into(),
            ))
            .into();
            assert!(status.admit_rejection(error.clone()));
            let closer = Arc::new(ConnectionCloser::new(status, handle.clone()));
            closer.noop();
            let acker =
                crate::acker::Acker::new(1, 7, Some(handle.clone()), None, KillSwitch::default());
            let mut caller: std::pin::Pin<Box<dyn Future<Output = Result<()>>>> = match operation {
                0 => Box::pin(handle.close_connection_checked(200, "local".into(), 0, 0)),
                1 => Box::pin(async { handle.create_channel(closer).await.map(|_| ()) }),
                2 => Box::pin(handle.update_secret("secret".into(), "reason".into())),
                3 => Box::pin(async { acker.ack(BasicAckOptions::default()).await.map(|_| ()) }),
                4 => Box::pin(async { acker.nack(BasicNackOptions::default()).await.map(|_| ()) }),
                5 => Box::pin(async {
                    acker
                        .reject(BasicRejectOptions::default())
                        .await
                        .map(|_| ())
                }),
                _ => unreachable!(),
            };
            assert!(
                futures_lite::future::poll_once(caller.as_mut())
                    .await
                    .is_none()
            );
            channels.set_connection_error(error);
            handle.task_scope.stop();
            drop(rpc);
            let result = futures_lite::future::poll_once(caller.as_mut())
                .await
                .expect("a retired RPC scope must settle its queued caller")
                .unwrap_err();
            assert!(
                matches!(result.kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503),
                "operation {operation}: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn rpc_retirement_propagates_a_ready_reply_before_cancelling_its_caller() {
        let runtime = Runtime::tokio_current();
        let status = crate::ConnectionStatus::default();
        let uri = crate::uri::AMQPUri::default();
        let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
        let socket = crate::socket_state::SocketState::default();
        let rpc = InternalRPC::new(
            runtime.clone(),
            Heartbeat::new(status.clone(), runtime.clone()),
            SecretUpdate::new(status.clone(), runtime, config.auth_provider.clone()),
            Frames::default(),
            socket.handle(),
            status.clone(),
        );
        let handle = rpc.handle();
        let (promise, resolver) = Promise::new("ready scoped reply");
        let task = handle.task_scope.wrap(async move {
            resolver.resolve(7);
        });
        let mut caller = Box::pin(handle.await_reply(promise));
        assert!(
            futures_lite::future::poll_once(caller.as_mut())
                .await
                .is_none()
        );
        handle.task_scope.stop();
        // The scope is stopping, but its ready task has not propagated yet.
        assert!(
            futures_lite::future::poll_once(caller.as_mut())
                .await
                .is_none()
        );
        task.await;
        assert_eq!(caller.await.unwrap(), 7);
    }

    pub(super) struct ReplyPollGap(Box<dyn FnOnce() + Send>);

    pub(super) fn after_pending_reply_poll(handle: &InternalRPCHandle) {
        let gap = handle.reply_poll_gap.lock().unwrap().take();
        if let Some(gap) = gap {
            (gap.0)();
        }
    }

    fn reserved_reply_handle() -> InternalRPCHandle {
        let runtime = Runtime::tokio_current();
        let status = crate::ConnectionStatus::default();
        let uri = crate::uri::AMQPUri::default();
        let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
        let socket = crate::socket_state::SocketState::default();
        let rpc = InternalRPC::new(
            runtime.clone(),
            Heartbeat::new(status.clone(), runtime.clone()),
            SecretUpdate::new(status.clone(), runtime, config.auth_provider.clone()),
            Frames::default(),
            socket.handle(),
            status.clone(),
        );
        let error = ErrorKind::ProtocolViolation(crate::protocol::AMQPError::new(
            crate::protocol::AMQPHardError::COMMANDINVALID.into(),
            "rejected frame".into(),
        ))
        .into();
        assert!(status.admit_rejection(error));
        rpc.handle()
    }

    async fn reply_completed_between_polls(result: Result<u8>) -> Result<u8> {
        let handle = reserved_reply_handle();
        let (promise, resolver) = Promise::new("reply completed between polls");
        let task = handle.task_scope.wrap(async move {
            resolver.complete(result);
        });
        handle.task_scope.stop();
        *handle.reply_poll_gap.lock().unwrap() = Some(ReplyPollGap(Box::new(move || {
            // The first promise poll was pending. Complete the producer and
            // release its final guard before the caller observes retirement.
            futures_lite::future::block_on(task);
        })));
        handle.await_reply(promise).await
    }

    #[tokio::test]
    async fn rpc_success_completed_during_retirement_wins() {
        assert_eq!(reply_completed_between_polls(Ok(7)).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn rpc_error_completed_during_retirement_wins() {
        let error = reply_completed_between_polls(Err(ErrorKind::MissingHeartbeatError.into()))
            .await
            .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::MissingHeartbeatError));
    }

    #[tokio::test]
    async fn rpc_completed_broken_pipe_keeps_its_result() {
        let handle = reserved_reply_handle();
        let (promise, resolver) = Promise::<()>::new("completed transport error");
        resolver.reject(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into());
        let error = handle.await_reply(promise).await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::IOError(cause)
            if cause.kind() == std::io::ErrorKind::BrokenPipe));
    }

    #[tokio::test]
    async fn rpc_abandoned_resolver_uses_the_reserved_rejection() {
        let handle = reserved_reply_handle();
        let (promise, resolver) = Promise::<()>::new("abandoned reply");
        drop(resolver);
        let error = handle.await_reply(promise).await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::ProtocolViolation(cause)
            if cause.get_id() == 503));
    }

    #[tokio::test]
    async fn a_full_internal_rpc_queue_reports_its_command_count() {
        let runtime = Runtime::tokio_current();
        let uri = crate::uri::AMQPUri::default();
        let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
        let status = crate::ConnectionStatus::new(&uri);
        let socket = crate::socket_state::SocketState::default();
        let frames = Frames::default();
        let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
        let secret_update = SecretUpdate::new(
            status.clone(),
            runtime.clone(),
            config.auth_provider.clone(),
        );
        let rpc = InternalRPC::new(
            runtime,
            heartbeat,
            secret_update,
            frames.clone(),
            socket.handle(),
            status.clone(),
        );
        let events = crate::events::Events::new();
        let mut listener = Box::pin(events.listener());
        let channels = Channels::new(
            config,
            status.clone(),
            socket.handle(),
            rpc.handle(),
            frames,
            events,
        );
        // Channel creation queued its status command already; fill the queue
        // before polling its owner, so the exhaustion is deterministic.
        let handle = rpc.handle();
        while !handle.sender.is_full() {
            handle.send_heartbeat();
        }
        handle.send_heartbeat();
        assert!(
            futures_lite::future::poll_once(rpc.run(channels))
                .await
                .is_some(),
            "the full RPC queue must end its owner without waiting"
        );
        assert_eq!(status.state(), crate::ConnectionState::Error);
        let crate::Event::Error(error) =
            futures_lite::future::poll_once(futures_lite::StreamExt::next(&mut listener))
                .await
                .expect("the owner must have published its error")
                .unwrap()
        else {
            panic!("RPC overflow must end the connection");
        };
        assert!(
            matches!(
                error.kind(),
                ErrorKind::ResourceLimitExceeded {
                    limit: crate::ResourceLimit::InternalRpcQueueMessages,
                    maximum: 256,
                    observed: Some(257),
                }
            ),
            "{error}"
        );
        assert!(!error.can_be_recovered());
    }

    #[tokio::test]
    async fn frame_rejection_keeps_its_verdict_until_the_close_write_settles() {
        use futures_lite::StreamExt;
        for competing in 0..6 {
            let runtime = Runtime::tokio_current();
            let uri = crate::uri::AMQPUri::default();
            let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
            let status = crate::ConnectionStatus::new(&uri);
            let socket = crate::socket_state::SocketState::default();
            let frames = Frames::default();
            let heartbeat = Heartbeat::new(status.clone(), runtime.clone());
            let secret_update = SecretUpdate::new(
                status.clone(),
                runtime.clone(),
                config.auth_provider.clone(),
            );
            let rpc = InternalRPC::new(
                runtime,
                heartbeat,
                secret_update,
                frames.clone(),
                socket.handle(),
                status.clone(),
            );
            let handle = rpc.handle();
            let events = crate::events::Events::new();
            let mut listener = Box::pin(events.listener());
            let channels = Channels::new(
                config,
                status.clone(),
                socket.handle(),
                handle.clone(),
                frames.clone(),
                events,
            );
            let error: Error = ErrorKind::ProtocolViolation(crate::protocol::AMQPError::new(
                crate::protocol::AMQPHardError::COMMANDINVALID.into(),
                "rejected frame".into(),
            ))
            .into();
            // Closed is already queued when receive admission reserves the end.
            if competing == 4 {
                handle.set_connection_closed(
                    ErrorKind::InvalidConnectionState(crate::ConnectionState::Closed).into(),
                );
            }
            assert!(status.admit_rejection(error.clone()));
            handle.reject_frame(0, error.clone(), 10, 50, None);
            let mut run = Box::pin(rpc.run(channels));
            assert!(
                futures_lite::future::poll_once(run.as_mut())
                    .await
                    .is_none()
            );
            if competing == 4 {
                assert!(status.errored());
                assert!(
                    matches!(frames.poison().unwrap().kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503)
                );
                handle.stop();
                run.await;
                continue;
            }
            let close = frames
                .pop(false)
                .expect("the close was admitted before poison");
            assert!(matches!(&*close,
                AMQPFrame::Method(0, crate::protocol::AMQPClass::Connection(crate::protocol::connection::AMQPMethod::Close(close)))
                if close.reply_code == 503));
            let (_, sending) = close.into_serialized_frame(1);
            match competing {
                0 => sending.resolve(),
                1 => sending.reject(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()),
                2 => handle.set_connection_closing(),
                3 => handle.init_connection_shutdown(
                    ErrorKind::InvalidConnectionState(crate::ConnectionState::Closed).into(),
                ),
                5 => handle.set_connection_error(
                    std::io::Error::from(std::io::ErrorKind::ConnectionAborted).into(),
                ),
                _ => unreachable!(),
            }
            let event = tokio::select! {
                _ = run.as_mut() => panic!("RPC owner retired before publishing the original verdict"),
                event = listener.next() => event.unwrap(),
            };
            let crate::Event::Error(actual) = event else {
                panic!("expected error event");
            };
            assert!(
                matches!(actual.kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503)
            );
            assert!(
                matches!(frames.poison().unwrap().kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503)
            );
            assert!(
                futures_lite::future::poll_once(listener.next())
                    .await
                    .is_none()
            );
            handle.stop();
            run.await;
        }
    }
}
