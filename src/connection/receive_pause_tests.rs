//! A full receive budget or event queue pauses reading until the program
//! takes what it holds (Boundless ADR-0192): nothing is refused, and reading
//! resumes with no heartbeat or other input to wake it.
use crate::{Connection, ConnectionProperties, ReceiveLimits};
use amq_protocol::{
    frame::{AMQPContentHeader, AMQPFrame, gen_frame, parse_frame},
    protocol::{AMQPClass, basic, channel, connection},
};
use async_rs::Runtime;
use futures_lite::StreamExt;
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn ends<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("owned peer stalled")
}
/// Waits for a state the connection reaches on its I/O thread.
async fn until(state: impl Fn() -> bool) {
    ends(async {
        while !state() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}
async fn write(stream: &mut TcpStream, frame: AMQPFrame) {
    let bytes = gen_frame(&frame)(Vec::new().into()).unwrap().into_inner().0;
    stream.write_all(&bytes).await.unwrap();
}
async fn send(stream: &mut TcpStream, id: u16, method: AMQPClass) {
    write(stream, AMQPFrame::Method(id, method)).await;
}
async fn read(stream: &mut TcpStream) -> AMQPFrame {
    let mut header = [0; 7];
    stream.read_exact(&mut header).await.unwrap();
    let n = u32::from_be_bytes(header[3..].try_into().unwrap()) as usize;
    assert!(n <= 8192);
    let mut bytes = header.to_vec();
    bytes.resize(n + 8, 0);
    stream.read_exact(&mut bytes[7..]).await.unwrap();
    let (rest, frame) = parse_frame(bytes.as_slice()).unwrap();
    assert!(rest.is_empty());
    frame
}
/// An established connection with heartbeats off and channel 1 open.
async fn established(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut header = [0; 8];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(&header, b"AMQP\0\0\x09\x01");
    send(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::Start(connection::Start {
            version_major: 0,
            version_minor: 9,
            mechanisms: "PLAIN".into(),
            locales: "en_US".into(),
            ..Default::default()
        })),
    )
    .await;
    assert!(matches!(
        read(&mut stream).await,
        AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::StartOk(_)))
    ));
    send(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::Tune(connection::Tune {
            channel_max: 8,
            frame_max: 8192,
            heartbeat: 0,
        })),
    )
    .await;
    assert!(matches!(
        read(&mut stream).await,
        AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::TuneOk(_)))
    ));
    assert!(matches!(
        read(&mut stream).await,
        AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Open(_)))
    ));
    send(
        &mut stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::OpenOk(connection::OpenOk {})),
    )
    .await;
    stream
}
/// Answers the client's Connection.Close, whatever it sent before it.
async fn close(stream: &mut TcpStream) {
    while !matches!(
        read(stream).await,
        AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(_)))
    ) {}
    send(
        stream,
        0,
        AMQPClass::Connection(connection::AMQPMethod::CloseOk(connection::CloseOk {})),
    )
    .await;
}
async fn open_channel(stream: &mut TcpStream) {
    assert!(matches!(
        read(stream).await,
        AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::Open(_)))
    ));
    send(
        stream,
        1,
        AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
    )
    .await;
}
async fn connect(address: std::net::SocketAddr, limits: Option<ReceiveLimits>) -> Connection {
    let mut options = ConnectionProperties::default().configure_backoff(|b| b.with_max_times(0));
    if let Some(limits) = limits {
        options = options.with_receive_limits(limits);
    }
    ends(Connection::connect_with_runtime(
        &format!("amqp://{address}/%2f"),
        options,
        Runtime::tokio_current(),
    ))
    .await
    .unwrap()
}

/// A delivery header with no room waits for it: dropping a delivery frees
/// room and wakes reading, and an RPC reply read behind the held header,
/// across the loop's later passes, arrives after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_delivery_header_resumes_once_a_delivery_is_dropped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (qos_seen, qos_read) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut stream = established(listener).await;
        open_channel(&mut stream).await;
        let AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Consume(consume))) =
            read(&mut stream).await
        else {
            panic!("Basic.Consume expected")
        };
        send(
            &mut stream,
            1,
            AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                consumer_tag: consume.consumer_tag.clone(),
            })),
        )
        .await;
        for tag in 1..=3u8 {
            send(
                &mut stream,
                1,
                AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                    consumer_tag: consume.consumer_tag.clone(),
                    delivery_tag: u64::from(tag),
                    redelivered: false,
                    exchange: "".into(),
                    routing_key: "held".into(),
                })),
            )
            .await;
            write(
                &mut stream,
                AMQPFrame::Header(
                    1,
                    AMQPContentHeader {
                        class_id: 60,
                        body_size: 1,
                        properties: Default::default(),
                    },
                ),
            )
            .await;
            write(&mut stream, AMQPFrame::Body(1, vec![tag])).await;
        }
        assert!(matches!(
            read(&mut stream).await,
            AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Qos(_)))
        ));
        qos_seen.send(()).unwrap();
        send(
            &mut stream,
            1,
            AMQPClass::Basic(basic::AMQPMethod::QosOk(basic::QosOk {})),
        )
        .await;
        close(&mut stream).await;
    });
    let connection = connect(
        address,
        Some(ReceiveLimits {
            message_bytes: 1,
            retained_bytes: 2,
            retained_messages: 2,
        }),
    )
    .await;
    let channel = ends(connection.create_channel()).await.unwrap();
    let mut consumer = ends(channel.basic_consume(
        "held".into(),
        "pause".into(),
        Default::default(),
        Default::default(),
    ))
    .await
    .unwrap();
    // Both deliveries keep their reservations: the third header has no room.
    let first = ends(consumer.next()).await.unwrap().unwrap();
    let second = ends(consumer.next()).await.unwrap().unwrap();
    assert_eq!(
        (first.data.as_slice(), second.data.as_slice()),
        (&[1][..], &[2][..])
    );
    let budget = connection
        .configuration
        .negotiated_config
        .budget
        .clone()
        .unwrap();
    until(|| budget.waiting()).await;
    // A write and later loop passes leave the held header waiting.
    let qos = tokio::spawn({
        let channel = channel.clone();
        async move { channel.basic_qos(1, Default::default()).await }
    });
    ends(qos_read).await.unwrap();
    assert!(budget.waiting());
    assert!(connection.status().connected());
    drop(first);
    let third = ends(consumer.next()).await.unwrap().unwrap();
    assert_eq!(third.data, [3]);
    ends(qos).await.unwrap().unwrap();
    assert!(connection.status().connected());
    drop((second, third));
    ends(connection.close(200, "done".into())).await.unwrap();
    ends(peer).await.unwrap();
}

/// A connection event that finds the event queue full is held, not an
/// overflow: reading the first queued event moves it in and wakes reading,
/// and every event arrives in order before the reply read behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_event_resumes_reading_once_an_event_is_read() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (unblocked, sent) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut stream = established(listener).await;
        assert!(matches!(
            read(&mut stream).await,
            AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::Open(_)))
        ));
        for _ in 0..70 {
            send(
                &mut stream,
                0,
                AMQPClass::Connection(connection::AMQPMethod::Unblocked(connection::Unblocked {})),
            )
            .await;
        }
        send(
            &mut stream,
            1,
            AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
        )
        .await;
        unblocked.send(()).unwrap();
        close(&mut stream).await;
    });
    let connection = connect(address, None).await;
    let mut events = Box::pin(connection.events_listener());
    let (opened, ()) = ends(async {
        tokio::join!(connection.create_channel(), async {
            sent.await.unwrap();
            // The 65th event waits in the held slot: no overflow is queued.
            until(|| connection.events.held()).await;
            for _ in 0..70 {
                assert!(matches!(
                    events.next().await.unwrap(),
                    crate::Event::ConnectionUnblocked
                ));
            }
        })
    })
    .await;
    let _channel = opened.unwrap();
    assert!(connection.status().connected());
    ends(connection.close(200, "done".into())).await.unwrap();
    ends(peer).await.unwrap();
}
