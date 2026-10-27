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
    fail: std::sync::atomic::AtomicBool,
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
        if self.gate.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
        }
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

#[derive(Clone, Copy, PartialEq)]
enum AuthArm {
    Start,
    Continue,
    Finish,
}
struct HeldAuth {
    arm: AuthArm,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    completed: tokio::sync::Notify,
    fail: bool,
}
impl HeldAuth {
    async fn run(&self, arm: AuthArm) -> Result<(), String> {
        if self.arm == arm {
            self.entered.notify_one();
            self.release.notified().await;
            self.completed.notify_one();
            if self.fail {
                return Err("late authentication failure".into());
            }
        }
        Ok(())
    }
}
struct HeldProvider(std::sync::Arc<HeldAuth>);
impl lapin::auth::AuthProvider for HeldProvider {
    fn mechanism(&self) -> lapin::types::ShortString {
        "PLAIN".into()
    }
    fn auth_starter_async(
        &self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<lapin::types::LongString, String>> + Send + '_>>
    {
        Box::pin(async { self.0.run(AuthArm::Start).await.map(|()| "".into()) })
    }
    fn continue_auth_async(
        &self,
        _: lapin::types::LongString,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<lapin::types::LongString, String>> + Send + '_>>
    {
        Box::pin(async { self.0.run(AuthArm::Continue).await.map(|()| "".into()) })
    }
    fn finish_auth_async(
        &self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(self.0.run(AuthArm::Finish))
    }
}
async fn close_during_auth(arm: AuthArm, fail: bool) {
    use std::{
        sync::{Arc, atomic::Ordering},
        task::Poll,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gate = Arc::new(WriteGate::default());
    let auth = Arc::new(HeldAuth {
        arm,
        entered: Default::default(),
        release: Default::default(),
        completed: Default::default(),
        fail,
    });
    let peer_gate = gate.clone();
    let peer_auth = auth.clone();
    let peer = tokio::spawn(async move {
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
        if arm != AuthArm::Start {
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::StartOk(_)))
            ));
            if arm == AuthArm::Continue {
                send(
                    &mut stream,
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Secure(connection::Secure {
                        challenge: "challenge".into(),
                    })),
                )
                .await;
            } else {
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
            }
        }
        peer_auth.entered.notified().await;
        peer_gate.hold.store(true, Ordering::SeqCst);
        send(
            &mut stream,
            0,
            AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close {
                reply_code: 530,
                reply_text: "peer closed during auth".into(),
                class_id: 10,
                method_id: 10,
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
        ConnectionProperties::default().with_auth_provider(HeldProvider(auth.clone())),
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
    auth.release.notify_one();
    ends(auth.completed.notified()).await;
    // This test uses one Tokio thread. After the provider returns, poison makes
    // the remaining auth-task awaits ready; its guard finishes before we resume.
    if result.is_none() {
        result = std::future::poll_fn(|cx| {
            Poll::Ready(match handshake.as_mut().poll(cx) {
                Poll::Ready(value) => Some(value),
                Poll::Pending => None,
            })
        })
        .await;
    }
    let early = result.is_some();
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
        !early,
        "authentication released the handshake owner before CloseOk"
    );
    assert!(
        matches!(error.kind(), ErrorKind::ProtocolError(error) if error.get_id() == 530 && error.get_message().as_str() == "peer closed during auth")
    );
}
#[tokio::test]
async fn starter_auth_close_waits_for_close_ok() {
    for fail in [false, true] {
        ends(close_during_auth(AuthArm::Start, fail)).await;
    }
}
#[tokio::test]
async fn continue_auth_close_waits_for_close_ok() {
    for fail in [false, true] {
        ends(close_during_auth(AuthArm::Continue, fail)).await;
    }
}
#[tokio::test]
async fn finish_auth_close_waits_for_close_ok() {
    for fail in [false, true] {
        ends(close_during_auth(AuthArm::Finish, fail)).await;
    }
}

// A connection method on an allocated nonzero channel must be rejected on
// the wire before the original protocol violation retires the native owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_connection_close_is_written_before_the_protocol_violation_retires_io() {
    for partial_get in [false, true] {
        for blocked in [false, true] {
            ends(rejected_connection_close(
                partial_get,
                blocked,
                RejectionAction::Write,
            ))
            .await;
        }
    }
}

#[derive(Clone, Copy)]
enum RejectionAction {
    Write,
    Deadline,
    Eof,
    LocalClose,
    PeerClose,
    Stop,
}

#[tokio::test]
async fn rejected_close_pending_write_has_a_deadline() {
    ends(rejected_connection_close(
        true,
        false,
        RejectionAction::Deadline,
    ))
    .await;
}
#[tokio::test]
async fn rejected_close_observes_peer_eof_during_write() {
    ends(rejected_connection_close(true, false, RejectionAction::Eof)).await;
}
#[tokio::test]
async fn rejected_close_wins_a_concurrent_local_close_and_partial_get() {
    ends(rejected_connection_close(
        true,
        false,
        RejectionAction::LocalClose,
    ))
    .await;
}
#[tokio::test]
async fn rejected_close_wins_a_later_peer_close() {
    ends(rejected_connection_close(
        true,
        false,
        RejectionAction::PeerClose,
    ))
    .await;
}
#[tokio::test]
async fn rejected_close_wins_explicit_driver_stop() {
    ends(rejected_connection_close(
        true,
        false,
        RejectionAction::Stop,
    ))
    .await;
}

async fn rejected_connection_close(partial_get: bool, blocked: bool, action: RejectionAction) {
    let (act, acted) = oneshot::channel();
    let (done, finished) = oneshot::channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gate = std::sync::Arc::new(WriteGate::default());
    let peer_gate = gate.clone();
    let peer = tokio::spawn(async move {
        let mut stream = handshake(listener, true).await;
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
        if partial_get {
            use amq_protocol::{frame::AMQPContentHeader, protocol::basic};
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(1, AMQPClass::Basic(basic::AMQPMethod::Get(_)))
            ));
            send(
                &mut stream,
                1,
                AMQPClass::Basic(basic::AMQPMethod::GetOk(basic::GetOk {
                    delivery_tag: 1,
                    redelivered: false,
                    exchange: "".into(),
                    routing_key: "pending".into(),
                    message_count: 0,
                })),
            )
            .await;
            let header = AMQPFrame::Header(
                1,
                AMQPContentHeader {
                    class_id: 60,
                    body_size: 1,
                    properties: Default::default(),
                },
            );
            let bytes = gen_frame(&header)(Vec::new().into())
                .unwrap()
                .into_inner()
                .0;
            stream.write_all(&bytes).await.unwrap();
        } else {
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(1, AMQPClass::Queue(queue::AMQPMethod::Declare(_)))
            ));
        }
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
        peer_gate
            .hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
        send(
            &mut stream,
            1,
            AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close {
                reply_code: 200,
                reply_text: "rejected peer close".into(),
                class_id: 50,
                method_id: 10,
            })),
        )
        .await;
        if !matches!(action, RejectionAction::Write) {
            acted.await.unwrap();
            if matches!(action, RejectionAction::Eof) {
                stream.shutdown().await.unwrap();
            } else if matches!(action, RejectionAction::PeerClose) {
                send(
                    &mut stream,
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close {
                        reply_code: 320,
                        reply_text: "later close".into(),
                        class_id: 0,
                        method_id: 0,
                    })),
                )
                .await;
            }
            // Keep the peer alive until the controller has seen the verdict.
            let _ = finished.await;
            return;
        }
        let AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(close))) =
            read(&mut stream).await
        else {
            panic!("expected local Connection.Close");
        };
        assert_eq!(close.reply_code, 503);
        assert_eq!((close.class_id, close.method_id), (10, 50));
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
    let connection = ends(handshake).await.unwrap();
    use futures_lite::StreamExt;
    let mut events = Box::pin(
        connection
            .events_listener()
            .filter(|event| matches!(event, lapin::Event::Error(_))),
    );
    let channel = ends(connection.create_channel()).await.unwrap();
    let rpc = async {
        if partial_get {
            channel
                .basic_get("pending".into(), Default::default())
                .await
                .map(|_| ())
        } else {
            channel
                .queue_declare("pending".into(), Default::default(), Default::default())
                .await
                .map(|_| ())
        }
    };
    tokio::pin!(rpc);
    let mut early = None;
    ends(std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(value) = rpc.as_mut().poll(cx) {
            early = Some(value);
            return std::task::Poll::Ready(());
        }
        if gate.entered.load(std::sync::atomic::Ordering::SeqCst) {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }))
    .await;
    let ended_before_write = early.is_some();
    match action {
        RejectionAction::Write => {
            gate.hold.store(false, std::sync::atomic::Ordering::SeqCst);
            if let Some(waker) = gate.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
        RejectionAction::Deadline => {
            act.send(()).unwrap();
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(11)).await;
            tokio::time::resume();
        }
        RejectionAction::Eof | RejectionAction::PeerClose => {
            act.send(()).unwrap();
        }
        RejectionAction::LocalClose => {
            act.send(()).unwrap();
            let close = connection.close(200, "local".into());
            tokio::pin!(close);
            // Admit the local close while the rejection write stays pending.
            let result = ends(close).await.unwrap_err();
            assert!(
                matches!(result.kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503)
            );
        }
        RejectionAction::Stop => {
            act.send(()).unwrap();
            owner.stop();
        }
    }
    let error = match early {
        Some(value) => value,
        None => ends(rpc).await,
    }
    .unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::ProtocolViolation(error)
        if error.get_id() == 503),
        "original protocol violation must win: {error:?}"
    );
    let lapin::Event::Error(event) = ends(events.next()).await.unwrap() else {
        panic!("expected winning error");
    };
    assert!(matches!(event.kind(), ErrorKind::ProtocolViolation(error) if error.get_id() == 503));
    assert!(
        futures_lite::future::poll_once(events.next())
            .await
            .is_none()
    );
    let _ = done.send(());
    // Joining is allowed only after the close write has settled; no caller stop
    // may be needed to complete this rejection.
    let _ = ends(tokio::task::spawn_blocking(move || owner.join()))
        .await
        .unwrap();
    ends(peer).await.unwrap();
    assert!(
        !ended_before_write,
        "the original end cannot retire a held close write"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_rejected_frames_write_the_requested_close_before_the_original_end() {
    use amq_protocol::{
        frame::AMQPContentHeader,
        protocol::{basic, confirm},
    };
    let header = |id| {
        AMQPFrame::Header(
            id,
            AMQPContentHeader {
                class_id: 60,
                body_size: 0,
                properties: Default::default(),
            },
        )
    };
    let mut instances = vec![
        (vec![AMQPFrame::InvalidHeartbeat(1)], 0, 501, 0, 0, false),
        (vec![header(0)], 0, 504, 60, 0, false),
        (vec![header(1)], 0, 505, 60, 0, false),
        (vec![AMQPFrame::Body(1, vec![1])], 0, 505, 0, 0, false),
        (
            vec![AMQPFrame::Method(
                1,
                AMQPClass::Queue(queue::AMQPMethod::PurgeOk(queue::PurgeOk {
                    message_count: 0,
                })),
            )],
            0,
            505,
            50,
            31,
            false,
        ),
    ];
    let multiple = false;
    instances.push((
        vec![AMQPFrame::Method(
            1,
            AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack {
                delivery_tag: 7,
                multiple,
            })),
        )],
        1,
        406,
        60,
        80,
        true,
    ));
    instances.push((
        vec![AMQPFrame::Method(
            1,
            AMQPClass::Basic(basic::AMQPMethod::Nack(basic::Nack {
                delivery_tag: 7,
                multiple,
                requeue: false,
            })),
        )],
        1,
        406,
        60,
        120,
        true,
    ));
    for (input, close_channel, code, class_id, method_id, confirms) in instances {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let mut stream = handshake(listener, true).await;
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
            if confirms {
                assert!(matches!(
                    read(&mut stream).await,
                    AMQPFrame::Method(1, AMQPClass::Confirm(confirm::AMQPMethod::Select(_)))
                ));
                send(
                    &mut stream,
                    1,
                    AMQPClass::Confirm(confirm::AMQPMethod::SelectOk(confirm::SelectOk {})),
                )
                .await;
            }
            assert!(matches!(
                read(&mut stream).await,
                AMQPFrame::Method(1, AMQPClass::Queue(queue::AMQPMethod::Declare(_)))
            ));
            for frame in input {
                let bytes = if matches!(frame, AMQPFrame::InvalidHeartbeat(_)) {
                    let mut bytes = gen_frame(&AMQPFrame::Heartbeat)(Vec::new().into())
                        .unwrap()
                        .into_inner()
                        .0;
                    bytes[2] = 1;
                    bytes
                } else {
                    gen_frame(&frame)(Vec::new().into()).unwrap().into_inner().0
                };
                stream.write_all(&bytes).await.unwrap();
            }
            let (id, close_code, close_class, close_method) = match read(&mut stream).await {
                AMQPFrame::Method(
                    id,
                    AMQPClass::Connection(connection::AMQPMethod::Close(close)),
                ) => (id, close.reply_code, close.class_id, close.method_id),
                AMQPFrame::Method(id, AMQPClass::Channel(channel::AMQPMethod::Close(close))) => {
                    (id, close.reply_code, close.class_id, close.method_id)
                }
                frame => panic!("expected local close: {frame:?}"),
            };
            assert_eq!(
                (id, close_code, close_class, close_method),
                (close_channel, code, class_id, method_id)
            );
        });
        let stream = ends(smol::net::TcpStream::connect(address)).await.unwrap();
        let (owner, handshake) = Connection::from_stream(
            format!("amqp://{address}/%2f").parse().unwrap(),
            Runtime::tokio_current(),
            stream,
            ConnectionProperties::default().with_auth_provider(Plain),
        )
        .unwrap();
        let connection = ends(handshake).await.unwrap();
        let channel = ends(connection.create_channel()).await.unwrap();
        if confirms {
            ends(channel.confirm_select(Default::default()))
                .await
                .unwrap();
        }
        let error =
            ends(channel.queue_declare("pending".into(), Default::default(), Default::default()))
                .await
                .unwrap_err();
        let _ = ends(tokio::task::spawn_blocking(move || owner.join()))
            .await
            .unwrap();
        ends(peer).await.unwrap();
        assert!(
            matches!(error.kind(), ErrorKind::ProtocolViolation(error)
            if error.get_id() == code),
            "{error:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_handshake_frame_keeps_its_verdict_through_a_failed_close_write() {
    use std::{
        sync::{Arc, atomic::Ordering},
        task::Poll,
    };
    for fail_write in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let gate = Arc::new(WriteGate::default());
        let peer_gate = gate.clone();
        let peer = tokio::spawn(async move {
            let mut stream = handshake(listener, false).await;
            peer_gate.hold.store(true, Ordering::SeqCst);
            let bytes = gen_frame(&AMQPFrame::InvalidHeartbeat(1))(Vec::new().into())
                .unwrap()
                .into_inner()
                .0;
            stream.write_all(&bytes).await.unwrap();
            if !fail_write {
                let AMQPFrame::Method(
                    0,
                    AMQPClass::Connection(connection::AMQPMethod::Close(close)),
                ) = read(&mut stream).await
                else {
                    panic!("expected local Close");
                };
                assert_eq!(close.reply_code, 501);
            }
            // Keep EOF behind the controller's write outcome. No tail shape is asserted.
            let mut drained = Vec::new();
            stream.read_to_end(&mut drained).await.unwrap();
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
        let mut early = None;
        ends(std::future::poll_fn(|cx| {
            if let Poll::Ready(value) = handshake.as_mut().poll(cx) {
                early = Some(value);
                return Poll::Ready(());
            }
            if gate.entered.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }))
        .await;
        let ended_before_write = early.is_some();
        gate.fail.store(fail_write, Ordering::SeqCst);
        gate.hold.store(false, Ordering::SeqCst);
        if let Some(waker) = gate.waker.lock().unwrap().take() {
            waker.wake();
        }
        let error = match early {
            Some(result) => result,
            None => ends(handshake).await,
        }
        .unwrap_err();
        let _ = ends(tokio::task::spawn_blocking(move || owner.join()))
            .await
            .unwrap();
        ends(peer).await.unwrap();
        assert!(!ended_before_write);
        assert!(
            matches!(error.kind(), ErrorKind::ProtocolViolation(error)
            if error.get_id() == 501),
            "{error:?}"
        );
    }
}
