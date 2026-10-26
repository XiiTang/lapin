#![cfg(feature = "tokio")]
use amq_protocol::{
    frame::{AMQPFrame, gen_frame, parse_frame},
    protocol::{AMQPClass, channel, connection, queue},
};
use async_rs::Runtime;
use lapin::{Connection, ConnectionProperties, ErrorKind};
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

async fn ends<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("owned peer stalled")
}
async fn send(stream: &mut TcpStream, id: u16, method: AMQPClass) {
    let bytes = gen_frame(&AMQPFrame::Method(id, method))(Vec::new().into())
        .unwrap()
        .into_inner()
        .0;
    stream.write_all(&bytes).await.unwrap();
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
async fn handshake(listener: TcpListener) -> TcpStream {
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

async fn peer_close(reply_code: u16, recover: bool, pending: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (go, gate) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut stream = handshake(listener).await;
        if pending {
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::Open(_)))
            ));
            send(
                &mut stream,
                1,
                AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
            )
            .await;
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(1, AMQPClass::Queue(queue::AMQPMethod::Declare(_)))
            ));
        }
        gate.await.unwrap();
        send(
            &mut stream,
            0,
            AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close {
                reply_code,
                reply_text: "owned peer close".into(),
                class_id: 50,
                method_id: 10,
            })),
        )
        .await;
        assert!(matches!(
            read(&mut stream).await,
            AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)))
        ));
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert!(
            rest.is_empty(),
            "CloseOk is the last and only close response"
        );
    });
    // Recovery exercises its shutdown branch, with no reconnect/retry attempt.
    let mut options = ConnectionProperties::default().configure_backoff(|b| b.with_max_times(0));
    if recover {
        options = options.enable_auto_recover();
    }
    let connection = ends(Connection::connect_with_runtime(
        &format!("amqp://{address}/%2f"),
        options,
        Runtime::tokio_current(),
    ))
    .await
    .unwrap();
    let channel = if pending {
        Some(ends(connection.create_channel()).await.unwrap())
    } else {
        None
    };
    let rpc = channel.as_ref().map(|channel| {
        channel.queue_declare("pending".into(), Default::default(), Default::default())
    });
    if let Some(rpc) = rpc {
        tokio::pin!(rpc);
        // Poll admission before allowing the peer to close; the peer also waits for Declare.
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(rpc.as_mut().poll(cx).is_pending()))
                .await
        );
        go.send(()).unwrap();
        let error = ends(rpc).await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::ProtocolError(error)
            if error.get_id() == reply_code && error.get_message().as_str() == "owned peer close"));
    } else {
        go.send(()).unwrap();
    }
    ends(peer).await.unwrap();
    assert!(ends(connection.create_channel()).await.is_err());
    let _ = ends(tokio::task::spawn_blocking(move || connection.run()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_connection_close_is_acknowledged_before_shutdown() {
    ends(peer_close(200, false, false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_connection_error_is_acknowledged_and_rejects_pending_work() {
    ends(peer_close(320, false, true)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_peer_close_is_acknowledged_before_retiring_transport() {
    ends(peer_close(320, true, false)).await;
}
