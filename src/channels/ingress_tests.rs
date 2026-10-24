use super::*;
use crate::{
    BasicProperties, Configuration, ConnectionProperties, Consumer, heartbeat::Heartbeat,
    internal_rpc::InternalRPC, options::*, secret_update::SecretUpdate, socket_state::SocketState,
    types::FieldTable,
};
use amq_protocol::frame::AMQPContentHeader;
use futures_core::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn fixture() -> (
    Channels,
    Frames,
    InternalRPC<crate::runtime::DefaultRuntimeKit>,
) {
    let runtime = crate::runtime::default_runtime().unwrap();
    let configuration = Configuration::new(
        &"amqp://localhost/%2f".parse().unwrap(),
        ConnectionProperties::default(),
    );
    configuration.negotiated_config.set_channel_max(8);
    let status = ConnectionStatus::default();
    status.set_state(ConnectionState::Connected);
    let frames = Frames::default();
    let socket = SocketState::default();
    let rpc = InternalRPC::new(
        runtime.clone(),
        Heartbeat::new(status.clone(), runtime.clone()),
        SecretUpdate::new(status.clone(), runtime, configuration.auth_provider.clone()),
        frames.clone(),
        socket.handle(),
    );
    let channels = Channels::new(
        configuration,
        status,
        socket.handle(),
        rpc.handle(),
        frames.clone(),
        Events::new(),
    );
    (channels, frames, rpc)
}

fn channel(channels: &Channels, rpc: &InternalRPC<crate::runtime::DefaultRuntimeKit>) -> Channel {
    let channel = channels
        .create(Arc::new(ConnectionCloser::new(
            channels.connection_status.clone(),
            rpc.handle(),
        )))
        .unwrap();
    channel.set_state(ChannelState::Connected);
    channel
}

fn consumer(
    channel: &Channel,
    rpc: &InternalRPC<crate::runtime::DefaultRuntimeKit>,
    tag: &str,
    no_ack: bool,
) -> Consumer {
    let consumer = Consumer::new(
        tag.into(),
        rpc.handle(),
        None,
        "queue".into(),
        BasicConsumeOptions {
            no_ack,
            ..Default::default()
        },
        FieldTable::default(),
    );
    channel.register_consumer(tag.into(), consumer.clone());
    consumer
}

fn deliver(channels: &Channels, channel: &Channel, tag: u64, consumer: &str) {
    channels
        .handle_frame(AMQPFrame::Method(
            channel.id(),
            AMQPClass::Basic(crate::protocol::basic::AMQPMethod::Deliver(
                crate::protocol::basic::Deliver {
                    consumer_tag: consumer.into(),
                    delivery_tag: tag,
                    redelivered: false,
                    exchange: "".into(),
                    routing_key: "queue".into(),
                },
            )),
        ))
        .unwrap();
}

fn content(channels: &Channels, channel: &Channel, data: &[u8]) {
    channels
        .handle_frame(AMQPFrame::Header(
            channel.id(),
            AMQPContentHeader {
                class_id: 60,
                body_size: data.len() as u64,
                properties: BasicProperties::default(),
            },
        ))
        .unwrap();
    if !data.is_empty() {
        channels
            .handle_frame(AMQPFrame::Body(channel.id(), data.into()))
            .unwrap();
    }
}

fn get_ok(channels: &Channels, channel: &Channel, tag: u64) {
    channels
        .handle_frame(AMQPFrame::Method(
            channel.id(),
            AMQPClass::Basic(crate::protocol::basic::AMQPMethod::GetOk(
                crate::protocol::basic::GetOk {
                    delivery_tag: tag,
                    redelivered: false,
                    exchange: "".into(),
                    routing_key: "queue".into(),
                    message_count: 0,
                },
            )),
        ))
        .unwrap();
}

fn next(consumer: &mut Consumer) -> Poll<Option<Result<crate::message::Delivery>>> {
    Pin::new(consumer).poll_next(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn freeze_seals_ingress_and_preserves_buffered_consumer_and_get_results() {
    let (channels, _frames, rpc) = fixture();
    let first = channel(&channels, &rpc);
    let second = channel(&channels, &rpc);
    let mut a = consumer(&first, &rpc, "a", false);
    let mut b = consumer(&first, &rpc, "b", true);
    let mut c = consumer(&second, &rpc, "c", false);
    deliver(&channels, &first, 2, "a");
    content(&channels, &first, b"a");
    deliver(&channels, &first, 13, "b");
    content(&channels, &first, b"b");
    let get = first
        .basic_get_completion("queue".into(), BasicGetOptions::default())
        .unwrap();
    get_ok(&channels, &first, 21);
    content(&channels, &first, b"get");
    deliver(&channels, &second, 7, "c");
    content(&channels, &second, b"");
    // Publisher-confirm tags are a different sequence and never enter the snapshot.
    let confirms: Vec<_> = (0..32)
        .map(|_| first.register_publisher_confirm())
        .collect();
    channels
        .handle_frame(AMQPFrame::Method(
            first.id(),
            AMQPClass::Basic(crate::protocol::basic::AMQPMethod::Ack(
                crate::protocol::basic::Ack {
                    delivery_tag: 32,
                    multiple: true,
                },
            )),
        ))
        .unwrap();
    let later_get = first
        .basic_get_completion("queue".into(), BasicGetOptions::default())
        .unwrap();
    let frozen = channels.freeze_ingress();
    assert!(channels.connection_status.connected());
    // These frames use the same native dispatcher as the I/O loop. A complete
    // post-freeze consumer delivery and get result must never be admitted.
    deliver(&channels, &first, 33, "a");
    content(&channels, &first, b"late");
    get_ok(&channels, &first, 34);
    content(&channels, &first, b"late get");
    for (consumer, tag, data) in [
        (&mut a, 2, b"a".as_slice()),
        (&mut b, 13, b"b".as_slice()),
        (&mut c, 7, b"".as_slice()),
    ] {
        match next(consumer) {
            Poll::Ready(Some(Ok(delivery))) => {
                assert_eq!(delivery.delivery_tag, tag);
                assert_eq!(delivery.generation, 0);
                assert_eq!(delivery.data, data);
            }
            result => panic!("accepted delivery lost: {result:?}"),
        }
        assert!(
            next(consumer).is_pending(),
            "post-freeze delivery reached the consumer"
        );
    }
    let message = get.try_take().unwrap().unwrap().unwrap();
    assert_eq!(message.delivery_tag, 21);
    assert_eq!(message.generation, 0);
    assert_eq!(message.data, b"get");
    assert!(
        later_get.try_take().is_none(),
        "post-freeze GetOk completed a get"
    );
    assert_eq!(frozen.get(&first.id()), Some(&21));
    assert_eq!(frozen.get(&second.id()), Some(&7));
    assert_eq!(channels.freeze_ingress(), frozen);
    // Retirement cannot replace an already completed broker result.
    channels.set_connection_error(ErrorKind::MissingHeartbeatError.into());
    assert_eq!(message.data, b"get");
    assert!(
        matches!(later_get.try_take(), Some(Err(error)) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
    );
    assert_eq!(channels.freeze_ingress(), frozen);
    drop(confirms);
}

#[test]
fn freeze_counts_partial_get_and_consume_tags_and_keeps_error_completion() {
    let (channels, _frames, rpc) = fixture();
    let first = channel(&channels, &rpc);
    let second = channel(&channels, &rpc);
    let mut consumer = consumer(&first, &rpc, "partial", false);
    deliver(&channels, &first, 13, "partial");
    channels
        .handle_frame(AMQPFrame::Header(
            first.id(),
            AMQPContentHeader {
                class_id: 60,
                body_size: 4,
                properties: BasicProperties::default(),
            },
        ))
        .unwrap();
    channels
        .handle_frame(AMQPFrame::Body(first.id(), vec![1, 2]))
        .unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let get = second
        .basic_get_completion("queue".into(), BasicGetOptions::default())
        .unwrap()
        .with_sink(move |result| {
            let _ = send.send(result);
        });
    get_ok(&channels, &second, 25);
    channels
        .handle_frame(AMQPFrame::Header(
            second.id(),
            AMQPContentHeader {
                class_id: 60,
                body_size: 4,
                properties: BasicProperties::default(),
            },
        ))
        .unwrap();
    channels
        .handle_frame(AMQPFrame::Body(second.id(), vec![1, 2]))
        .unwrap();
    let frozen = channels.freeze_ingress();
    channels
        .handle_frame(AMQPFrame::Body(first.id(), vec![3, 4]))
        .unwrap();
    channels
        .handle_frame(AMQPFrame::Body(second.id(), vec![3, 4]))
        .unwrap();
    assert!(next(&mut consumer).is_pending());
    assert!(get.try_take().is_none());
    assert_eq!(frozen.get(&first.id()), Some(&13));
    assert_eq!(frozen.get(&second.id()), Some(&25));
    drop(get);
    channels.set_connection_error(ErrorKind::MissingHeartbeatError.into());
    assert!(
        matches!(recv.try_recv(), Ok(Err(error)) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
    );
    assert!(recv.try_recv().is_err());
    assert_eq!(channels.freeze_ingress(), frozen);
}

#[test]
fn freeze_keeps_the_maximum_tag_even_after_native_channel_removal() {
    let (channels, _frames, rpc) = fixture();
    let channel = channel(&channels, &rpc);
    let mut consumer = consumer(&channel, &rpc, "maximum", true);
    deliver(&channels, &channel, u64::MAX, "maximum");
    content(&channels, &channel, b"");
    channels
        .remove(channel.id(), ErrorKind::MissingHeartbeatError.into())
        .unwrap();
    let frozen = channels.freeze_ingress();
    assert_eq!(frozen.get(&channel.id()), Some(&u64::MAX));
    assert!(
        matches!(next(&mut consumer), Poll::Ready(Some(Ok(delivery))) if delivery.delivery_tag == u64::MAX)
    );
    assert_eq!(channels.freeze_ingress(), frozen);
}

#[test]
fn freeze_reports_the_current_lifetime_when_a_native_channel_id_is_reused() {
    let (channels, _frames, rpc) = fixture();
    channels.configuration.negotiated_config.set_channel_max(1);
    let old = channel(&channels, &rpc);
    let mut old_consumer = consumer(&old, &rpc, "old", false);
    deliver(&channels, &old, 13, "old");
    content(&channels, &old, b"old body");
    old.set_closed(ErrorKind::MissingHeartbeatError.into());
    channels
        .remove(old.id(), ErrorKind::MissingHeartbeatError.into())
        .unwrap();
    let replacement = channel(&channels, &rpc);
    assert_eq!(replacement.id(), old.id());
    let mut new_consumer = consumer(&replacement, &rpc, "new", false);
    deliver(&channels, &replacement, 1, "new");
    content(&channels, &replacement, b"new body");
    let frozen = channels.freeze_ingress();
    assert_eq!(
        frozen.get(&replacement.id()),
        Some(&1),
        "an ended channel lifetime inflated its replacement's high-water mark"
    );
    assert!(
        matches!(next(&mut old_consumer), Poll::Ready(Some(Ok(delivery))) if delivery.delivery_tag == 13 && delivery.data == b"old body")
    );
    assert!(
        matches!(next(&mut new_consumer), Poll::Ready(Some(Ok(delivery))) if delivery.delivery_tag == 1 && delivery.data == b"new body")
    );
    // Creation after freeze cannot mutate the immutable retirement snapshot.
    replacement.set_closed(ErrorKind::MissingHeartbeatError.into());
    channels
        .remove(replacement.id(), ErrorKind::MissingHeartbeatError.into())
        .unwrap();
    let _unused = channel(&channels, &rpc);
    assert_eq!(channels.freeze_ingress(), frozen);
}

#[test]
fn freeze_after_candidate_failure_keeps_its_tags_and_complete_data() {
    let (channels, _frames, rpc) = fixture();
    let channel = channel(&channels, &rpc);
    let mut consumer = consumer(&channel, &rpc, "failed-candidate", true);
    deliver(&channels, &channel, 13, "failed-candidate");
    content(&channels, &channel, b"old candidate");
    let get = channel
        .basic_get_completion("queue".into(), BasicGetOptions::default())
        .unwrap();
    get_ok(&channels, &channel, 15);
    content(&channels, &channel, b"candidate get");
    channels.set_connection_error(ErrorKind::MissingHeartbeatError.into());
    let frozen = channels.freeze_ingress();
    assert_eq!(frozen.get(&channel.id()), Some(&15));
    assert!(
        matches!(next(&mut consumer), Poll::Ready(Some(Ok(delivery))) if delivery.delivery_tag == 13 && delivery.generation == 0 && delivery.data == b"old candidate")
    );
    assert!(
        matches!(next(&mut consumer), Poll::Ready(Some(Err(error))) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
    );
    let message = get.try_take().unwrap().unwrap().unwrap();
    assert_eq!(message.delivery_tag, 15);
    assert_eq!(message.generation, 0);
    assert_eq!(message.data, b"candidate get");
    assert_eq!(channels.freeze_ingress(), frozen);
}

// The test below additionally forces an in-flight handoff to hold the exact
// dispatch mutex while another thread freezes it. No scheduling delay is used
// to infer that the critical section is occupied.
#[test]
fn freeze_joins_an_in_flight_native_dispatch_before_returning_the_snapshot() {
    use std::{
        sync::{Mutex, mpsc},
        task::Wake,
        time::Duration,
    };
    struct HeldWake {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Wake for HeldWake {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(15))
                .expect("dispatch handoff was never released");
        }
    }
    let (channels, _frames, rpc) = fixture();
    let channel = channel(&channels, &rpc);
    let mut consumer = consumer(&channel, &rpc, "held", false);
    let (entered_send, entered_recv) = mpsc::channel();
    let (release_send, release_recv) = mpsc::channel();
    let waker = Waker::from(Arc::new(HeldWake {
        entered: entered_send,
        release: Mutex::new(release_recv),
    }));
    assert!(
        Pin::new(&mut consumer)
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let dispatch_channels = channels.clone();
    let dispatch_channel = channel.clone();
    let dispatch = std::thread::spawn(move || {
        deliver(&dispatch_channels, &dispatch_channel, 13, "held");
        content(&dispatch_channels, &dispatch_channel, b"held");
    });
    entered_recv
        .recv_timeout(Duration::from_secs(15))
        .expect("native dispatch never reached its consumer handoff");
    assert!(
        channels.ingress.try_lock().is_err(),
        "native dispatch bypassed the freeze checkpoint"
    );
    let freeze_channels = channels.clone();
    let (done_send, done_recv) = mpsc::channel();
    let freeze =
        std::thread::spawn(move || done_send.send(freeze_channels.freeze_ingress()).unwrap());
    release_send.send(()).unwrap();
    let frozen = done_recv
        .recv_timeout(Duration::from_secs(15))
        .expect("freeze did not join the in-flight dispatch");
    dispatch.join().unwrap();
    freeze.join().unwrap();
    assert_eq!(frozen.get(&channel.id()), Some(&13));
    assert!(
        matches!(next(&mut consumer), Poll::Ready(Some(Ok(delivery))) if delivery.data == b"held")
    );
    deliver(&channels, &channel, 14, "held");
    content(&channels, &channel, b"late");
    assert!(next(&mut consumer).is_pending());
}
