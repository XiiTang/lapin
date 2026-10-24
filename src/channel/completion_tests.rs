use super::topology_collection_tests::channel;
use super::*;
use std::sync::mpsc;

#[test]
fn rpc_completion_reaches_the_sink_before_or_after_waiter_drop() {
    for ready in [false, true] {
        let (channel, _rpc) = channel(crate::ConnectionProperties::default());
        let (send, recv) = mpsc::channel();
        let completion = channel
            .queue_declare_completion(
                "".into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .unwrap()
            .with_sink(move |result| send.send(result).unwrap());
        assert!(channel.frames.pop(true).is_some());
        let reply = || {
            channel
                .receive_method(AMQPClass::Queue(protocol::queue::AMQPMethod::DeclareOk(
                    protocol::queue::DeclareOk {
                        queue: "peer-name".into(),
                        message_count: 7,
                        consumer_count: 3,
                    },
                )))
                .unwrap()
        };
        if ready {
            reply();
        }
        drop(completion);
        if !ready {
            reply();
        }
        let queue = recv.try_recv().unwrap().unwrap();
        assert_eq!(queue.name().as_str(), "peer-name");
        assert_eq!(queue.message_count(), 7);
        assert_eq!(queue.consumer_count(), 3);
        channel.set_connection_error(ErrorKind::MissingHeartbeatError.into());
        assert!(recv.try_recv().is_err());
    }
}

#[test]
fn get_completion_keeps_body_reservation_empty_and_error_results() {
    for ready in [false, true] {
        for outcome in 0..3 {
            let (channel, _rpc) = channel(
                crate::ConnectionProperties::default().with_receive_limits(crate::ReceiveLimits {
                    message_bytes: 4,
                    retained_bytes: 4,
                    retained_messages: 1,
                }),
            );
            let (send, recv) = mpsc::channel();
            let completion = channel
                .basic_get_completion("queue".into(), BasicGetOptions::default())
                .unwrap()
                .with_sink(move |result| send.send(result).unwrap());
            assert!(channel.frames.pop(true).is_some());
            let reply = || match outcome {
                0 => {
                    channel
                        .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::GetOk(
                            protocol::basic::GetOk {
                                delivery_tag: 13,
                                redelivered: true,
                                exchange: "".into(),
                                routing_key: "queue".into(),
                                message_count: 2,
                            },
                        )))
                        .unwrap();
                    channel
                        .handle_content_header_frame(60, 4, BasicProperties::default())
                        .unwrap();
                    channel.handle_body_frame(vec![1, 2]).unwrap();
                    assert!(recv.try_recv().is_err());
                    channel.handle_body_frame(vec![3, 4]).unwrap();
                }
                1 => channel
                    .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::GetEmpty(
                        protocol::basic::GetEmpty {},
                    )))
                    .unwrap(),
                _ => {
                    // The native owner cancels expected replies when a physical
                    // connection fails; this harness does not run its RPC pump.
                    let error: Error = ErrorKind::MissingHeartbeatError.into();
                    channel.set_connection_error(error.clone());
                    channel.frames.clear_expected_replies(channel.id, error);
                }
            };
            if ready {
                reply();
            }
            drop(completion);
            if !ready {
                reply();
            }
            let result = recv.try_recv().unwrap();
            match outcome {
                0 => {
                    let message = result.unwrap().unwrap();
                    assert_eq!(message.delivery_tag, 13);
                    assert_eq!(message.generation, channel.status.generation());
                    assert_eq!(message.data, [1, 2, 3, 4]);
                    let budget = channel.configuration.budget.as_ref().unwrap();
                    assert!(budget.reserve(1).is_err());
                    channel.set_connection_error(ErrorKind::MissingHeartbeatError.into());
                    assert_eq!(message.data, [1, 2, 3, 4]);
                    drop(message);
                    assert!(budget.reserve(4).is_ok());
                }
                1 => assert!(matches!(result, Ok(None))),
                _ => assert!(
                    matches!(result, Err(error) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
                ),
            }
            assert!(recv.try_recv().is_err());
        }
    }
}

#[test]
fn confirm_completion_keeps_ack_nack_returned_message_and_error() {
    for ready in [false, true] {
        for outcome in 0..3 {
            let (channel, _rpc) = channel(crate::ConnectionProperties::default());
            let (send, recv) = mpsc::channel();
            let completion = channel
                .register_publisher_confirm()
                .into_completion()
                .with_sink(move |result| send.send(result).unwrap());
            let reply = || {
                if outcome == 2 {
                    channel.set_connection_error(ErrorKind::MissingHeartbeatError.into());
                    return;
                }
                channel
                    .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::Return(
                        protocol::basic::Return {
                            reply_code: 312,
                            reply_text: "NO_ROUTE".into(),
                            exchange: "".into(),
                            routing_key: "missing".into(),
                        },
                    )))
                    .unwrap();
                channel
                    .handle_content_header_frame(60, 4, BasicProperties::default())
                    .unwrap();
                channel.handle_body_frame(vec![1, 2, 3, 4]).unwrap();
                if outcome == 0 {
                    channel
                        .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::Ack(
                            protocol::basic::Ack {
                                delivery_tag: 1,
                                multiple: false,
                            },
                        )))
                        .unwrap();
                } else {
                    channel
                        .receive_method(AMQPClass::Basic(protocol::basic::AMQPMethod::Nack(
                            protocol::basic::Nack {
                                delivery_tag: 1,
                                multiple: false,
                                requeue: false,
                            },
                        )))
                        .unwrap();
                }
            };
            if ready {
                reply();
            }
            drop(completion);
            if !ready {
                reply();
            }
            let result = recv.try_recv().unwrap();
            if outcome == 2 {
                assert!(
                    matches!(result, Err(error) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
                );
            } else {
                let confirmation = result.unwrap();
                assert_eq!(confirmation.is_ack(), outcome == 0);
                assert_eq!(confirmation.is_nack(), outcome == 1);
                let message = confirmation.take_message().unwrap();
                assert_eq!(message.reply_code, 312);
                assert_eq!(message.data, [1, 2, 3, 4]);
            }
            assert!(recv.try_recv().is_err());
            assert!(channel.take_dropped_confirms().is_empty());
        }
    }
}

#[test]
fn abandoned_publish_write_hands_its_confirm_to_the_owner() {
    let (channel, _rpc) = channel(crate::ConnectionProperties::default());
    channel.status.set_confirm();
    let (send, recv) = mpsc::channel();
    let completion = channel
        .basic_publish_completion(
            "".into(),
            "queue".into(),
            BasicPublishOptions::default(),
            b"body",
            BasicProperties::default(),
        )
        .unwrap()
        .with_sink(move |result| send.send(result).unwrap());
    drop(completion);
    // Serialization ownership advances only when the real frame owner resolves it.
    while let Some(frame) = channel.frames.pop(true) {
        frame.into_serialized_frame(1).1.resolve();
    }
    let confirm = recv.try_recv().unwrap().unwrap();
    drop(confirm);
    let mut confirms = channel.take_dropped_confirms();
    assert_eq!(confirms.len(), 1);
    let (send, recv) = mpsc::channel();
    drop(
        confirms
            .pop()
            .unwrap()
            .with_sink(move |result| send.send(result).unwrap()),
    );
    channel.acknowledgements.nack(1).unwrap();
    assert!(matches!(recv.try_recv(), Ok(Ok(Confirmation::Nack(None)))));
    assert!(channel.take_dropped_confirms().is_empty());
}
