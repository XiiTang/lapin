use super::*;
use crate::{
    BasicProperties, ChannelState, ConnectionState, ErrorKind, Event, RemoteClose,
    options::{BasicConsumeOptions, BasicGetOptions, BasicQosOptions, QueueDeclareOptions},
    protocol::{AMQPClass, basic, channel, connection},
    secret_update::SecretUpdate,
    types::FieldTable,
};
use futures_core::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

struct Fixture {
    connection: Connection,
    channels: Channels,
    rpc: InternalRPCHandle,
    events: Events,
    frames: Frames,
}

impl Fixture {
    fn new() -> Self {
        let runtime = Runtime::tokio_current();
        let config = Configuration::new(&AMQPUri::default(), ConnectionProperties::default());
        config.negotiated_config.set_channel_max(8);
        let status = ConnectionStatus::default();
        status.set_state(ConnectionState::Connected);
        let frames = Frames::default();
        let socket = SocketState::default();
        let rpc = InternalRPC::new(
            runtime.clone(),
            Heartbeat::new(status.clone(), runtime.clone()),
            SecretUpdate::new(status.clone(), runtime, config.auth_provider.clone()),
            frames.clone(),
            socket.handle(),
        );
        let handle = rpc.handle();
        handle
            .explicit_close
            .store(true, std::sync::atomic::Ordering::Release);
        let events = Events::new();
        events.bind(handle.clone());
        let channels = Channels::new(
            config.clone(),
            status.clone(),
            socket.handle(),
            handle.clone(),
            frames.clone(),
            events.clone(),
        );
        channels.channel0().set_state(ChannelState::Connected);
        let connection = Connection::new(config, status, handle.clone(), events.clone());
        connection.closer.noop();
        rpc.start(channels.clone());
        Self {
            connection,
            channels,
            rpc: handle,
            events,
            frames,
        }
    }

    fn channel(&self) -> Channel {
        let channel = self
            .channels
            .create(self.connection.closer.clone(), None)
            .unwrap();
        channel.set_state(ChannelState::Connected);
        channel
    }

    async fn acknowledge_connection_close(&self) {
        // These transportless fixtures own the physical write completion.
        // Shutdown first removes ordinary queued work and admits only CloseOk.
        while self.frames.poison().is_none() {
            tokio::task::yield_now().await;
        }
        let frame = loop {
            if let Some(frame) = self.frames.pop(true) {
                break frame;
            }
            tokio::task::yield_now().await;
        };
        assert!(matches!(
            &*frame,
            AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)))
        ));
        frame.into_serialized_frame(1).1.resolve();
        while !self.connection.status().closed() && !self.connection.status().errored() {
            tokio::task::yield_now().await;
        }
    }

    async fn stop(&self) {
        self.rpc.stop();
        self.rpc.task_scope.stop();
        let scope = self.rpc.task_scope.clone();
        tokio::task::spawn_blocking(move || scope.wait())
            .await
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.rpc.stop();
        self.rpc.task_scope.stop();
    }
}

fn check(error: &Error, expected: &RemoteClose) {
    assert_eq!(
        error.remote_close(),
        Some(expected),
        "Close lost by {error:?}"
    );
    assert_eq!(error.clone().remote_close(), Some(expected));
    assert!(!error.to_string().contains("private peer text"));
    if let ErrorKind::ProtocolError(amqp) = error.kind() {
        let (code, text) = match expected {
            RemoteClose::Connection(close) => (close.reply_code, &close.reply_text),
            RemoteClose::Channel(close) => (close.reply_code, &close.reply_text),
        };
        assert_eq!(amqp.get_id(), code);
        assert_eq!(amqp.get_message(), text);
    }
}

fn partial_get(channel: &Channel) -> crate::Completion<Option<crate::message::BasicGetMessage>> {
    let get = channel
        .basic_get_completion("queue".into(), BasicGetOptions::default())
        .unwrap();
    channel
        .receive_method(AMQPClass::Basic(basic::AMQPMethod::GetOk(basic::GetOk {
            delivery_tag: 7,
            redelivered: false,
            exchange: "".into(),
            routing_key: "key".into(),
            message_count: 0,
        })))
        .unwrap();
    channel
        .handle_content_header_frame(60, 4, BasicProperties::default())
        .unwrap();
    channel.handle_body_frame(b"ab".to_vec()).unwrap();
    get
}

#[tokio::test]
async fn remote_connection_close_reaches_channels_and_pending_completions() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for reply_code in [200, 599, 320, 406] {
            let fixture = Fixture::new();
            let mut events = Box::pin(fixture.events.listener());
            let first = fixture.channel();
            let second = fixture.channel();
            let queue = first
                .queue_declare_completion(
                    "queue".into(),
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .unwrap();
            let get = partial_get(&second);
            let qos = second
                .basic_qos_completion(2, BasicQosOptions::default())
                .unwrap();
            let confirm = first.register_publisher_confirm();
            let close = connection::Close {
                reply_code,
                reply_text: "private peer text".into(),
                class_id: 60,
                method_id: 40,
            };
            let expected = RemoteClose::Connection(close.clone());
            fixture
                .channels
                .handle_frame(AMQPFrame::Method(
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Close(close)),
                ))
                .unwrap();
            fixture.acknowledge_connection_close().await;
            let error = queue.await.unwrap_err();
            check(&error, &expected);
            assert_eq!(error.is_amqp_error(), matches!(reply_code, 320 | 406));
            assert_eq!(error.is_amqp_soft_error(), reply_code == 406);
            assert_eq!(error.is_amqp_hard_error(), reply_code == 320);
            check(&get.await.unwrap_err(), &expected);
            check(&qos.await.unwrap_err(), &expected);
            check(&confirm.await.unwrap_err(), &expected);
            for channel in [&first, &second] {
                check(
                    &channel
                        .basic_qos_completion(1, BasicQosOptions::default())
                        .unwrap_err(),
                    &expected,
                );
            }
            check(
                &fixture.connection.create_channel_completion().unwrap_err(),
                &expected,
            );
            let failed = matches!(reply_code, 320 | 406);
            assert_eq!(fixture.connection.status().errored(), failed);
            assert_eq!(fixture.connection.status().closed(), !failed);
            let mut cx = Context::from_waker(Waker::noop());
            match events.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Event::Error(error))) if failed => check(&error, &expected),
                Poll::Pending if !failed => {}
                other => panic!("Close changed connection event emission: {other:?}"),
            }
            fixture.stop().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_channel_close_keeps_normal_cancellation_and_all_error_facts() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for reply_code in [200, 599, 406, 320] {
            let fixture = Fixture::new();
            let mut events = Box::pin(fixture.events.listener());
            let channel = fixture.channel();
            let independent = fixture.channel();
            let consume = channel
                .basic_consume_completion(
                    "queue".into(),
                    "tag".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .unwrap();
            channel
                .receive_method(AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(
                    basic::ConsumeOk {
                        consumer_tag: "tag".into(),
                    },
                )))
                .unwrap();
            let mut consumer = consume.await.unwrap();
            let get = partial_get(&channel);
            let queue = channel
                .queue_declare_completion(
                    "queue".into(),
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .unwrap();
            let confirm = channel.register_publisher_confirm();
            let close = channel::Close {
                reply_code,
                reply_text: "private peer text".into(),
                class_id: 0,
                method_id: 17,
            };
            let expected = RemoteClose::Channel(close.clone());
            fixture
                .channels
                .handle_frame(AMQPFrame::Method(
                    channel.id(),
                    AMQPClass::Channel(channel::AMQPMethod::Close(close)),
                ))
                .unwrap();
            let error = get.await.unwrap_err();
            check(&error, &expected);
            let known = matches!(reply_code, 406 | 320);
            assert_eq!(error.is_amqp_error(), known);
            assert_eq!(error.is_amqp_soft_error(), reply_code == 406);
            assert_eq!(error.is_amqp_hard_error(), reply_code == 320);
            if !known {
                assert!(matches!(
                    error.kind(),
                    ErrorKind::InvalidChannelState(ChannelState::Closing, "channel closing")
                ));
            }
            check(&queue.await.unwrap_err(), &expected);
            check(&confirm.await.unwrap_err(), &expected);
            let mut cx = Context::from_waker(Waker::noop());
            let item = Pin::new(&mut consumer).poll_next(&mut cx);
            if known {
                match item {
                    Poll::Ready(Some(Err(error))) => check(&error, &expected),
                    other => panic!("missing consumer error: {other:?}"),
                }
            } else {
                assert!(matches!(item, Poll::Ready(None)));
            }
            match events.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(Event::Error(error))) if known => check(&error, &expected),
                Poll::Pending if !known => {}
                other => panic!("Close changed event emission: {other:?}"),
            }
            check(
                &channel
                    .basic_qos_completion(1, BasicQosOptions::default())
                    .unwrap_err(),
                &expected,
            );
            assert!(independent.status().connected());
            assert!(fixture.connection.status().connected());
            fixture.stop().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_close_completes_an_abandoned_handshake_with_its_peer_facts() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for reply_code in [599, 403] {
            let fixture = Fixture::new();
            let connection = Connection::new(
                fixture.connection.configuration.clone(),
                fixture.connection.status.clone(),
                fixture.rpc.clone(),
                fixture.events.clone(),
            );
            connection.closer.noop();
            let handshake = connection
                .start_completion(fixture.channels.channel0())
                .unwrap();
            let (send, receive) = tokio::sync::oneshot::channel();
            drop(handshake.with_sink(move |result| {
                let _ = send.send(result);
            }));
            let close = connection::Close {
                reply_code,
                reply_text: "private peer text".into(),
                class_id: 10,
                method_id: 40,
            };
            let expected = RemoteClose::Connection(close.clone());
            fixture
                .channels
                .handle_frame(AMQPFrame::Method(
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Close(close)),
                ))
                .unwrap();
            fixture.acknowledge_connection_close().await;
            let error = receive.await.unwrap().unwrap_err();
            check(&error, &expected);
            assert_eq!(error.is_amqp_error(), reply_code == 403);
            fixture.stop().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_close_context_ends_at_the_replacement_generation() {
    let fixture = Fixture::new();
    let channel = fixture.channel();
    let close = connection::Close {
        reply_code: 320,
        reply_text: "private peer text".into(),
        class_id: 0,
        method_id: 0,
    };
    let expected = RemoteClose::Connection(close.clone());
    let pending = channel
        .basic_qos_completion(1, BasicQosOptions::default())
        .unwrap();
    fixture
        .channels
        .handle_frame(AMQPFrame::Method(
            0,
            AMQPClass::Connection(connection::AMQPMethod::Close(close)),
        ))
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap_err();
    // Join the real dispatcher before inspecting a replacement's status: the
    // old Close-Ok task must not race these deliberately driven transitions.
    fixture.stop().await;
    fixture.connection.status.set_reconnecting();
    channel.init_recovery(error.clone());
    check(&channel.status().state_error("during recovery"), &expected);
    fixture.connection.status.set_connecting().unwrap();
    assert!(
        fixture
            .connection
            .status
            .ensure_connected()
            .unwrap_err()
            .remote_close()
            .is_none()
    );
    // The retained recovery cause still belongs to the retired generation.
    check(&channel.status().state_error("during recovery"), &expected);
    channel.finalize_connection();
    channel.set_closing(None);
    assert!(
        channel
            .status()
            .state_error("local close")
            .remote_close()
            .is_none()
    );
    check(&error, &expected);
}

#[tokio::test]
async fn connection_close_on_nonzero_channel_retains_native_facts() {
    let fixture = Fixture::new();
    let channel = fixture.channel();
    let pending = channel
        .basic_qos_completion(1, BasicQosOptions::default())
        .unwrap();
    let close = connection::Close {
        reply_code: 599,
        reply_text: "private peer text".into(),
        class_id: 0,
        method_id: 0,
    };
    let expected = RemoteClose::Connection(close.clone());
    let direct = fixture
        .channels
        .handle_frame(AMQPFrame::Method(
            channel.id(),
            AMQPClass::Connection(connection::AMQPMethod::Close(close)),
        ))
        .unwrap_err();
    assert!(matches!(direct.kind(), ErrorKind::ProtocolViolation(_)));
    // The real I/O owner forwards dispatch failure to the connection shutdown.
    fixture.channels.set_connection_error(direct.clone());
    let completion = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap_err();
    fixture.stop().await;
    assert_eq!(
        (direct.remote_close(), completion.remote_close()),
        (Some(&expected), Some(&expected)),
        "dispatcher error and pending completion",
    );
}

#[tokio::test]
async fn close_routing_and_state_validation_keep_native_facts() {
    for connection_scope in [true, false] {
        for missing in [true, false] {
            let fixture = Fixture::new();
            let channel = fixture.channel();
            let pending = channel
                .basic_qos_completion(1, BasicQosOptions::default())
                .unwrap();
            let (method, expected) = if connection_scope {
                let close = connection::Close {
                    reply_code: 599,
                    reply_text: "private peer text".into(),
                    class_id: 0,
                    method_id: 17,
                };
                (
                    AMQPClass::Connection(connection::AMQPMethod::Close(close.clone())),
                    RemoteClose::Connection(close),
                )
            } else {
                let close = channel::Close {
                    reply_code: 599,
                    reply_text: "private peer text".into(),
                    class_id: 0,
                    method_id: 17,
                };
                (
                    AMQPClass::Channel(channel::AMQPMethod::Close(close.clone())),
                    RemoteClose::Channel(close),
                )
            };
            let id = if missing {
                7
            } else if connection_scope {
                0
            } else {
                channel.id()
            };
            if !missing {
                fixture
                    .channels
                    .get(id)
                    .unwrap()
                    .set_state(ChannelState::Closed);
            }
            let direct = fixture
                .channels
                .handle_frame(AMQPFrame::Method(id, method))
                .unwrap_err();
            if missing {
                assert!(matches!(direct.kind(), ErrorKind::InvalidChannel(7)));
            } else {
                assert!(matches!(
                    direct.kind(),
                    ErrorKind::InvalidChannelState(ChannelState::Closed, _)
                ));
            }
            fixture.channels.set_connection_error(direct.clone());
            let completion = tokio::time::timeout(Duration::from_secs(10), pending)
                .await
                .unwrap()
                .unwrap_err();
            fixture.stop().await;
            check(&direct, &expected);
            check(&completion, &expected);
            check(
                &fixture.connection.create_channel_completion().unwrap_err(),
                &expected,
            );
        }
    }
}

#[tokio::test]
async fn queued_settlements_after_channel_close_keep_native_facts() {
    let fixture = Fixture::new();
    let channel = fixture.channel();
    let pending = channel
        .basic_qos_completion(1, BasicQosOptions::default())
        .unwrap();
    let close = channel::Close {
        reply_code: 406,
        reply_text: "private peer text".into(),
        class_id: 60,
        method_id: 40,
    };
    let expected = RemoteClose::Channel(close.clone());
    fixture
        .channels
        .handle_frame(AMQPFrame::Method(
            channel.id(),
            AMQPClass::Channel(channel::AMQPMethod::Close(close)),
        ))
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap_err();
    assert!(fixture.channels.get(channel.id()).is_none());
    for operation in 0..3 {
        let (pending, resolver) = Promise::new("queued settlement");
        match operation {
            0 => fixture.rpc.basic_ack(
                channel.id(),
                1,
                crate::options::BasicAckOptions::default(),
                resolver,
                None,
            ),
            1 => fixture.rpc.basic_nack(
                channel.id(),
                1,
                crate::options::BasicNackOptions::default(),
                resolver,
                None,
            ),
            _ => fixture.rpc.basic_reject(
                channel.id(),
                1,
                crate::options::BasicRejectOptions::default(),
                resolver,
                None,
            ),
        }
        let error = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::InvalidChannel(id) if *id == channel.id()));
        check(&error, &expected);
    }
    fixture.stop().await;
}

#[tokio::test]
async fn get_settlement_after_rpc_exit_keeps_peer_close_on_resolver_loss() {
    let fixture = Fixture::new();
    let channel = fixture.channel();
    let get = partial_get(&channel);
    channel.handle_body_frame(b"cd".to_vec()).unwrap();
    let message = get.await.unwrap().unwrap();
    let pending = channel
        .basic_qos_completion(1, BasicQosOptions::default())
        .unwrap();
    let close = connection::Close {
        reply_code: 200,
        reply_text: "private peer text".into(),
        class_id: 0,
        method_id: 0,
    };
    let expected = RemoteClose::Connection(close.clone());
    fixture
        .channels
        .handle_frame(AMQPFrame::Method(
            0,
            AMQPClass::Connection(connection::AMQPMethod::Close(close)),
        ))
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap_err();
    fixture.stop().await;
    drop(channel);
    drop(fixture);
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        message.ack(crate::options::BasicAckOptions::default()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::IOError(cause) if cause.kind() == std::io::ErrorKind::BrokenPipe)
    );
    assert_eq!(
        error.to_string(),
        "IO error: promise lost its last resolver before completion"
    );
    check(&error, &expected);
}

#[tokio::test]
async fn first_received_close_envelope_is_retained_by_status_and_error() {
    use crate::error::ReceivedClose;
    let method = || {
        RemoteClose::Connection(connection::Close {
            reply_code: 200,
            reply_text: "first".into(),
            class_id: 0,
            method_id: 0,
        })
    };
    let first = Arc::new(ReceivedClose::new(
        method(),
        Some(Box::new(std::io::Error::other("first context"))),
    ));
    let second = Arc::new(ReceivedClose::new(
        method(),
        Some(Box::new(std::io::Error::other("second context"))),
    ));
    let fixture = Fixture::new();
    let channel = fixture.channel().status().clone();
    let connection = fixture.connection.status().clone();
    assert!(Arc::ptr_eq(
        &connection.set_remote_close(first.clone()),
        &first
    ));
    assert!(Arc::ptr_eq(
        &connection.set_remote_close(second.clone()),
        &first
    ));
    assert!(Arc::ptr_eq(&connection.remote_close().unwrap(), &first));
    assert!(Arc::ptr_eq(
        &channel.set_remote_close(first.clone()),
        &first
    ));
    assert!(Arc::ptr_eq(
        &channel.set_remote_close(second.clone()),
        &first
    ));
    let error = Error::from(ErrorKind::InvalidChannel(1))
        .with_remote_close(Some(first.clone()))
        .with_remote_close(Some(second));
    assert!(Arc::ptr_eq(&error.remote_close_shared().unwrap(), &first));
    assert!(std::ptr::eq(
        error.remote_close_context().unwrap(),
        error.clone().remote_close_context().unwrap()
    ));
    fixture.stop().await;
}
