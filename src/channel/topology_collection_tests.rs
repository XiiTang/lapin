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

fn channel(
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
            .with_recovery(true, true)
            .with_topology_collection(false),
    ] {
        // No metadata fits: a successful result must not depend on topology admission.
        let (channel, _rpc) = channel(properties.with_topology_limit(0));
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
