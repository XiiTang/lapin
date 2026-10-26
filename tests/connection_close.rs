#![cfg(feature = "tokio")]
use amq_protocol::{
    frame::{AMQPFrame, gen_frame, parse_frame},
    protocol::{AMQPClass, channel, connection, queue},
};
use async_rs::Runtime;
use futures_io::{AsyncRead, AsyncWrite};
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
async fn handshake(listener: TcpListener, established: bool) -> TcpStream {
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
    if established {
        send(
            &mut stream,
            0,
            AMQPClass::Connection(connection::AMQPMethod::OpenOk(connection::OpenOk {})),
        )
        .await;
    }
    stream
}

async fn peer_close(reply_code: u16, recover: bool, pending: bool, blocked: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (go, gate) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut stream = handshake(listener, true).await;
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
        if blocked {
            send(
                &mut stream,
                0,
                AMQPClass::Connection(connection::AMQPMethod::Blocked(connection::Blocked {
                    reason: "resource alarm".into(),
                })),
            )
            .await;
        }
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
    ends(peer_close(200, false, false, false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_connection_error_is_acknowledged_and_rejects_pending_work() {
    ends(peer_close(320, false, true, false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_peer_close_is_acknowledged_before_retiring_transport() {
    ends(peer_close(320, true, false, false)).await;
}

// A failed handshake's caller immediately stops its owned driver. Hold the
// CloseOk write to prove that rejection cannot race ahead of that write.
#[derive(Default)]
struct WriteGate {
    hold: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicBool,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}
struct GatedStream {
    stream: smol::net::TcpStream,
    gate: std::sync::Arc<WriteGate>,
}
impl AsyncRead for GatedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, bytes)
    }
}
impl AsyncWrite for GatedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut waker = self.gate.waker.lock().unwrap();
        if self.gate.hold.load(std::sync::atomic::Ordering::SeqCst) {
            *waker = Some(cx.waker().clone());
            self.gate
                .entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return std::task::Poll::Pending;
        }
        drop(waker);
        std::pin::Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_close(cx)
    }
}
struct Plain;
impl lapin::auth::AuthProvider for Plain {
    fn mechanism(&self) -> lapin::types::ShortString {
        "PLAIN".into()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handshake_close_is_written_before_its_error_releases_the_owner() {
    use std::{
        sync::{Arc, atomic::Ordering},
        task::Poll,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gate = Arc::new(WriteGate::default());
    let peer_gate = gate.clone();
    let peer = tokio::spawn(async move {
        let mut stream = handshake(listener, false).await;
        peer_gate.hold.store(true, Ordering::SeqCst);
        send(
            &mut stream,
            0,
            AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close {
                reply_code: 530,
                reply_text: "vhost refused".into(),
                class_id: 10,
                method_id: 40,
            })),
        )
        .await;
        assert!(matches!(
            read(&mut stream).await,
            AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)))
        ));
    });
    let stream = ends(smol::net::TcpStream::connect(address)).await.unwrap();
    let (owner, handshake) = Connection::from_stream(
        format!("amqp://{address}/%2f").parse().unwrap(),
        Runtime::tokio_current(),
        GatedStream {
            stream,
            gate: gate.clone(),
        },
        ConnectionProperties::default().with_auth_provider(Plain),
    )
    .unwrap();
    tokio::pin!(handshake);
    let mut result = None;
    ends(std::future::poll_fn(|cx| {
        if result.is_none()
            && let Poll::Ready(value) = handshake.as_mut().poll(cx)
        {
            result = Some(value);
        }
        if gate.entered.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }))
    .await;
    // Observe the promise after the I/O thread reached the held write.
    if result.is_none() {
        result = std::future::poll_fn(|cx| {
            Poll::Ready(match handshake.as_mut().poll(cx) {
                Poll::Ready(value) => Some(value),
                Poll::Pending => None,
            })
        })
        .await;
    }
    let rejected_before_write = result.is_some();
    gate.hold.store(false, Ordering::SeqCst);
    if let Some(waker) = gate.waker.lock().unwrap().take() {
        waker.wake();
    }
    let error = match result {
        Some(result) => result,
        None => ends(handshake).await,
    }
    .unwrap_err();
    owner.stop();
    let _ = ends(tokio::task::spawn_blocking(move || owner.join()))
        .await
        .unwrap();
    ends(peer).await.unwrap();
    assert!(
        !rejected_before_write,
        "the handshake's owner could stop I/O before CloseOk"
    );
    assert!(
        matches!(error.kind(), ErrorKind::ProtocolError(error) if error.get_id() == 530 && error.get_message().as_str() == "vhost refused")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_peer_connection_close_is_acknowledged() {
    ends(peer_close(200, false, false, true)).await;
}
