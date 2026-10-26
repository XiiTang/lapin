use super::*;
use crate::{
    ConnectionProperties, configuration::Configuration, events::Events, heartbeat::Heartbeat,
    internal_rpc::InternalRPC, secret_update::SecretUpdate, socket_state::SocketState,
};
use futures_core::Stream;
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
};

pub(super) fn channel(
    properties: ConnectionProperties,
) -> (Channel, InternalRPC<crate::runtime::DefaultRuntimeKit>) {
    let runtime = crate::runtime::default_runtime().unwrap();
    let configuration = Configuration::new(&"amqp://localhost/%2f".parse().unwrap(), properties);
    let status = ConnectionStatus::default();
    let socket = SocketState::default();
    let frames = Frames::default();
    let rpc = InternalRPC::new(
        runtime.clone(),
        Heartbeat::new(status.clone(), runtime.clone()),
        SecretUpdate::new(status.clone(), runtime, configuration.auth_provider.clone()),
        frames.clone(),
        socket.handle(),
        status.clone(),
    );
    let recovery = configuration.recovery_config();
    let channel = Channel::new(
        1,
        configuration.negotiated_config,
        status,
        socket.handle(),
        rpc.handle(),
        frames,
        None,
        recovery,
        Events::new().sender(),
        None,
        None,
    );
    channel.set_state(ChannelState::Connected);
    (channel, rpc)
}

// Exercise the public request, expected-reply dispatch and caller's future without a broker.
fn reply<T: fmt::Debug>(
    channel: &Channel,
    request: impl Future<Output = Result<T>>,
    response: AMQPClass,
) -> T {
    let mut request = pin!(request);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(request.as_mut().poll(&mut cx).is_pending());
    assert!(
        channel.frames.pop(true).is_some(),
        "request was not dispatched"
    );
    channel.receive_method(response).unwrap();
    match request.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(value)) => value,
        result => panic!("broker result did not reach caller: {result:?}"),
    }
}

#[test]
fn disabled_collection_preserves_broker_results_routing_and_empty_recovery_snapshots() {
    for properties in [
        ConnectionProperties::default().with_topology_collection(false),
        ConnectionProperties::default()
            .with_topology_collection(false)
            .enable_auto_recover(),
        ConnectionProperties::default()
            .enable_auto_recover()
            .with_topology_collection(false),
    ] {
        let (channel, _rpc) = channel(properties);
        reply(
            &channel,
            channel.exchange_declare(
                "declared".into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            ),
            AMQPClass::Exchange(protocol::exchange::AMQPMethod::DeclareOk(
                protocol::exchange::DeclareOk {},
            )),
        );
        let queue = reply(
            &channel,
            channel.queue_declare(
                "".into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            ),
            AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
                protocol::queue::DeclareOk {
                    queue: "server-name".into(),
                    message_count: 7,
                    consumer_count: 3,
                },
            )),
        );
        assert_eq!(queue.name().as_str(), "server-name");
        assert_eq!(queue.message_count(), 7);
        assert_eq!(queue.consumer_count(), 3);
        // These destinations are external: no declaration preceded either bind.
        reply(
            &channel,
            channel.exchange_bind(
                "external-exchange".into(),
                "external-source".into(),
                "key".into(),
                ExchangeBindOptions::default(),
                FieldTable::default(),
            ),
            AMQPClass::Exchange(protocol::exchange::AMQPMethod::BindOk(
                protocol::exchange::BindOk {},
            )),
        );
        reply(
            &channel,
            channel.queue_bind(
                "external-queue".into(),
                "external-source".into(),
                "key".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            ),
            AMQPClass::Queue(protocol::queue::AMQPMethod::BindOk(
                protocol::queue::BindOk {},
            )),
        );
        let mut consumer = reply(
            &channel,
            channel.basic_consume(
                "external-queue".into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            ),
            AMQPClass::Basic(protocol::basic::AMQPMethod::ConsumeOk(
                protocol::basic::ConsumeOk {
                    consumer_tag: "server-tag".into(),
                },
            )),
        );
        assert_eq!(consumer.tag().as_str(), "server-tag");
        channel
            .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::Deliver(
                protocol::basic::Deliver {
                    consumer_tag: "server-tag".into(),
                    delivery_tag: 1,
                    redelivered: false,
                    exchange: "external-source".into(),
                    routing_key: "key".into(),
                },
            )))
            .unwrap();
        channel
            .handle_content_header_frame(60, 4, BasicProperties::default())
            .unwrap();
        channel.handle_body_frame(b"data".to_vec()).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        match std::pin::Pin::new(&mut consumer).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(delivery))) => assert_eq!(delivery.data, b"data"),
            result => panic!("ConsumeOk did not establish live routing: {result:?}"),
        }
        let topology = channel.topology();
        assert!(topology.exchanges.is_empty());
        assert!(topology.queues.is_empty());
        assert_eq!(topology.consumers.len(), 1);
        if channel.recovery_config.0 {
            channel.init_recovery(ErrorKind::MissingHeartbeatError.into());
            let snapshot = channel.update_recovery().unwrap();
            assert!(snapshot.exchanges.is_empty());
            assert!(snapshot.queues.is_empty());
            assert_eq!(snapshot.consumers.len(), 1);
        }
    }
}

#[test]
fn default_collection_still_retains_topology() {
    let (channel, _rpc) = channel(ConnectionProperties::default());
    reply(
        &channel,
        channel.exchange_declare(
            "declared".into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Exchange(protocol::exchange::AMQPMethod::DeclareOk(
            protocol::exchange::DeclareOk {},
        )),
    );
    reply(
        &channel,
        channel.queue_declare(
            "queue".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
            protocol::queue::DeclareOk {
                queue: "queue".into(),
                ..Default::default()
            },
        )),
    );
    let topology = channel.topology();
    assert_eq!(topology.exchanges.len(), 1);
    assert!(topology.exchanges[0].is_declared);
    assert_eq!(topology.queues.len(), 1);
    assert!(topology.queues[0].is_declared);
}

#[test]
fn qos_replies_complete_without_adding_recovery_work() {
    let (channel, _rpc) = channel(ConnectionProperties::default().enable_auto_recover());
    for global in [false, true] {
        reply(
            &channel,
            channel.basic_qos(7, BasicQosOptions { global }),
            AMQPClass::Basic(protocol::basic::AMQPMethod::QosOk(
                protocol::basic::QosOk {},
            )),
        );
    }
    channel.init_recovery(ErrorKind::MissingHeartbeatError.into());
    let mut recovery = pin!(channel.start_recovery());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    assert!(channel.frames.pop(true).is_some());
    channel
        .receive_method(AMQPClass::Channel(protocol::channel::AMQPMethod::OpenOk(
            protocol::channel::OpenOk {},
        )))
        .unwrap();
    assert!(
        matches!(recovery.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
        "recovery unexpectedly replays previously acknowledged QoS"
    );
    assert!(channel.frames.pop(true).is_none());
}

async fn recover(channel: &Channel) -> Result<()> {
    channel.start_recovery().await
}

#[test]
fn native_recovery_uses_retained_queue_names_and_literal_bindings() {
    let (channel, _rpc) = channel(ConnectionProperties::default().enable_auto_recover());
    let queue = reply(
        &channel,
        channel.queue_declare(
            "".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
            protocol::queue::DeclareOk {
                queue: "broker-name".into(),
                message_count: 2,
                consumer_count: 3,
            },
        )),
    );
    assert_eq!(
        (
            queue.name().as_str(),
            queue.message_count(),
            queue.consumer_count()
        ),
        ("broker-name", 2, 3)
    );
    reply(
        &channel,
        channel.queue_bind(
            "broker-name".into(),
            "external".into(),
            "broker-name".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Queue(protocol::queue::AMQPMethod::BindOk(
            protocol::queue::BindOk {},
        )),
    );
    channel.init_recovery(ErrorKind::MissingHeartbeatError.into());
    let mut recovery = pin!(recover(&channel));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    assert!(matches!(
        &*channel.frames.pop(true).unwrap(),
        AMQPFrame::Method(
            1,
            AMQPClass::Channel(protocol::channel::AMQPMethod::Open(_))
        )
    ));
    channel
        .receive_method(AMQPClass::Channel(protocol::channel::AMQPMethod::OpenOk(
            protocol::channel::OpenOk {},
        )))
        .unwrap();
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    let declaration = channel.frames.pop(true).unwrap();
    match &*declaration {
        AMQPFrame::Method(1, AMQPClass::Queue(protocol::queue::AMQPMethod::Declare(method))) => {
            assert_eq!(
                method.queue.as_str(),
                "broker-name",
                "native recovery must use upstream retained-name behavior"
            )
        }
        other => panic!("expected queue declaration: {other:?}"),
    }
    channel
        .receive_method(AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
            protocol::queue::DeclareOk {
                queue: "broker-name".into(),
                ..Default::default()
            },
        )))
        .unwrap();
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    let binding = channel.frames.pop(true).unwrap();
    match &*binding {
        AMQPFrame::Method(1, AMQPClass::Queue(protocol::queue::AMQPMethod::Bind(method))) => {
            assert_eq!(
                (method.queue.as_str(), method.routing_key.as_str()),
                ("broker-name", "broker-name")
            )
        }
        other => panic!("expected queue binding: {other:?}"),
    }
    channel
        .receive_method(AMQPClass::Queue(protocol::queue::AMQPMethod::BindOk(
            protocol::queue::BindOk {},
        )))
        .unwrap();
    assert!(matches!(
        recovery.as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(channel.frames.pop(true).is_none());
}

#[test]
fn successful_topology_replies_are_independent_of_delivery_capacity() {
    let properties = ConnectionProperties::default().with_receive_limits(crate::ReceiveLimits {
        message_bytes: 0,
        retained_bytes: 0,
        retained_messages: 0,
    });
    let (channel, _rpc) = channel(properties);
    let queue = reply(
        &channel,
        channel.queue_declare(
            "".into(),
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
            protocol::queue::DeclareOk {
                queue: "returned-name".into(),
                message_count: 7,
                consumer_count: 3,
            },
        )),
    );
    assert_eq!(
        (
            queue.name().as_str(),
            queue.message_count(),
            queue.consumer_count()
        ),
        ("returned-name", 7, 3)
    );
    reply(
        &channel,
        channel.exchange_declare(
            "exchange".into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Exchange(protocol::exchange::AMQPMethod::DeclareOk(
            protocol::exchange::DeclareOk {},
        )),
    );
    reply(
        &channel,
        channel.queue_bind(
            "returned-name".into(),
            "exchange".into(),
            "key".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Queue(protocol::queue::AMQPMethod::BindOk(
            protocol::queue::BindOk {},
        )),
    );
    reply(
        &channel,
        channel.exchange_bind(
            "external".into(),
            "exchange".into(),
            "key".into(),
            ExchangeBindOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Exchange(protocol::exchange::AMQPMethod::BindOk(
            protocol::exchange::BindOk {},
        )),
    );
    let topology = channel.topology();
    assert_eq!(topology.queues.len(), 1);
    assert_eq!(topology.queues[0].bindings.len(), 1);
    assert_eq!(topology.exchanges.len(), 2);
    assert_eq!(
        topology
            .exchanges
            .iter()
            .map(|ex| ex.bindings.len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn upstream_recovery_restores_the_existing_consumer_and_tag() {
    let (channel, _rpc) = channel(ConnectionProperties::default().enable_auto_recover());
    let mut consumer = reply(
        &channel,
        channel.basic_consume(
            "external-queue".into(),
            "kept-tag".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ),
        AMQPClass::Basic(protocol::basic::AMQPMethod::ConsumeOk(
            protocol::basic::ConsumeOk {
                consumer_tag: "kept-tag".into(),
            },
        )),
    );
    channel.init_recovery(ErrorKind::MissingHeartbeatError.into());
    let mut recovery = pin!(channel.start_recovery());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    assert!(channel.frames.pop(true).is_some());
    channel
        .receive_method(AMQPClass::Channel(protocol::channel::AMQPMethod::OpenOk(
            protocol::channel::OpenOk {},
        )))
        .unwrap();
    assert!(recovery.as_mut().poll(&mut cx).is_pending());
    let consume = channel.frames.pop(true).unwrap();
    match &*consume {
        AMQPFrame::Method(1, AMQPClass::Basic(protocol::basic::AMQPMethod::Consume(method))) => {
            assert_eq!(
                (method.queue.as_str(), method.consumer_tag.as_str()),
                ("external-queue", "kept-tag")
            )
        }
        other => panic!("expected restored consume: {other:?}"),
    }
    channel
        .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::ConsumeOk(
            protocol::basic::ConsumeOk {
                consumer_tag: "kept-tag".into(),
            },
        )))
        .unwrap();
    assert!(matches!(
        recovery.as_mut().poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    channel
        .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::Deliver(
            protocol::basic::Deliver {
                consumer_tag: "kept-tag".into(),
                delivery_tag: 1,
                redelivered: false,
                exchange: "".into(),
                routing_key: "key".into(),
            },
        )))
        .unwrap();
    channel
        .handle_content_header_frame(60, 4, BasicProperties::default())
        .unwrap();
    channel.handle_body_frame(b"data".to_vec()).unwrap();
    match std::pin::Pin::new(&mut consumer).poll_next(&mut cx) {
        Poll::Ready(Some(Ok(delivery))) => {
            assert_eq!(delivery.data, b"data");
            assert_eq!(delivery.generation, 1);
        }
        other => panic!("restored delivery did not reach the existing consumer: {other:?}"),
    }
    assert!(channel.frames.pop(true).is_none());
}

#[test]
fn assert_channel0_dispatches_one_unchanged_protocol_rejection() {
    let (channel, _rpc) = channel(ConnectionProperties::default());
    let error = channel.assert_channel0(70, 20).unwrap_err();
    let ErrorKind::ProtocolViolation(native) = error.kind() else {
        panic!("protocol violation: {error:?}")
    };
    assert_eq!(native.get_id(), 503);
    assert_eq!(
        native.get_message().as_str(),
        "connection frame received on channel 1"
    );
    assert_eq!(&*channel.close_dispatch.lock().unwrap(), &[(70, 20)]);
}

#[test]
fn a_rejected_connection_close_is_observed_before_its_close_dispatch() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Observer {
        dispatch: Arc<std::sync::Mutex<Vec<(Identifier, Identifier)>>>,
        calls: AtomicUsize,
    }
    impl crate::RemoteCloseObserver for Observer {
        fn received(
            &self,
            close: &RemoteClose,
            accepted: bool,
        ) -> Option<Box<dyn std::error::Error + Send + Sync>> {
            assert!(!accepted);
            assert!(self.dispatch.lock().unwrap().is_empty());
            let RemoteClose::Connection(close) = close else {
                panic!("physical observer")
            };
            assert_eq!(close.reply_code, 200);
            assert_eq!((close.class_id, close.method_id), (0, 0));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Some(Box::new(std::io::Error::other("received connection close")))
        }
    }
    struct ChildObserver(AtomicUsize);
    impl crate::RemoteCloseObserver for ChildObserver {
        fn received(
            &self,
            _: &RemoteClose,
            _: bool,
        ) -> Option<Box<dyn std::error::Error + Send + Sync>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            None
        }
    }
    let (mut channel, _rpc) = channel(ConnectionProperties::default());
    let observer = Arc::new(Observer {
        dispatch: channel.close_dispatch.clone(),
        calls: AtomicUsize::new(0),
    });
    let child_observer = Arc::new(ChildObserver(AtomicUsize::new(0)));
    channel.connection_close_observer = Some(observer.clone());
    channel.remote_close_observer = Some(child_observer.clone());
    let error = channel
        .receive_method(AMQPClass::Connection(
            protocol::connection::AMQPMethod::Close(protocol::connection::Close {
                reply_code: 200,
                reply_text: "stop".into(),
                class_id: 0,
                method_id: 0,
            }),
        ))
        .unwrap_err();
    assert!(matches!(error.kind(), ErrorKind::ProtocolViolation(_)));
    assert_eq!(
        error.remote_close_context().unwrap().to_string(),
        "received connection close"
    );
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(child_observer.0.load(Ordering::SeqCst), 0);
    assert_eq!(&*channel.close_dispatch.lock().unwrap(), &[(10, 50)]);
}
