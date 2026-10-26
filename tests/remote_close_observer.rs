#![cfg(feature = "tokio")]
use amq_protocol::{frame::{AMQPFrame, gen_frame, parse_frame}, protocol::{AMQPClass, channel, connection, queue}};
use async_rs::Runtime;
use lapin::{Connection, ConnectionProperties, RemoteClose, RemoteCloseObserver};
use std::{error::Error, fmt, future::Future, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, time::{Duration, Instant}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc};

const STALL: Duration = Duration::from_secs(60);
async fn ends<T>(future: impl Future<Output = T>) -> T { tokio::time::timeout(STALL, future).await.expect("owned fixture stalled") }
#[derive(Debug, Clone)]
struct Report(Arc<()>);
impl fmt::Display for Report { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("received Close") } }
impl Error for Report {}
struct Observer {
    calls: AtomicUsize,
    release: Arc<AtomicBool>,
    entered: mpsc::UnboundedSender<(RemoteClose, bool)>,
    report: Report,
}
impl RemoteCloseObserver for Observer {
    fn received(&self, close: &RemoteClose, accepted: bool) -> Option<Box<dyn Error + Send + Sync>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send((close.clone(), accepted)).unwrap();
        let deadline = Instant::now() + STALL;
        while !self.release.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "observer release state never reached");
            std::thread::yield_now();
        }
        Some(Box::new(self.report.clone()))
    }
}
fn make_observer() -> (Arc<Observer>, mpsc::UnboundedReceiver<(RemoteClose, bool)>) {
    let (entered, seen) = mpsc::unbounded_channel();
    (Arc::new(Observer { calls: AtomicUsize::new(0), release: Arc::new(AtomicBool::new(false)), entered, report: Report(Arc::new(())) }), seen)
}
fn context(error: &lapin::Error, report: &Report) {
    let held = error.remote_close_context().unwrap().downcast_ref::<Report>().unwrap();
    assert!(Arc::ptr_eq(&held.0, &report.0));
    let cloned = error.clone();
    assert!(std::ptr::eq(error.remote_close_context().unwrap(), cloned.remote_close_context().unwrap()));
}
async fn send(stream: &mut TcpStream, id: u16, method: AMQPClass) {
    let bytes = gen_frame(&AMQPFrame::Method(id, method))(Vec::new().into()).unwrap().into_inner().0;
    stream.write_all(&bytes).await.unwrap();
}
async fn read(stream: &mut TcpStream) -> AMQPFrame {
    let mut header = [0; 7]; stream.read_exact(&mut header).await.unwrap();
    let n = u32::from_be_bytes(header[3..].try_into().unwrap()) as usize;
    assert!(n <= 8192);
    let mut bytes = header.to_vec(); bytes.resize(n + 8, 0); stream.read_exact(&mut bytes[7..]).await.unwrap();
    let (rest, frame) = parse_frame(bytes.as_slice()).unwrap(); assert!(rest.is_empty()); frame
}
async fn handshake(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut header = [0; 8]; stream.read_exact(&mut header).await.unwrap(); assert_eq!(&header, b"AMQP\0\0\x09\x01");
    send(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::Start(connection::Start {
        version_major: 0, version_minor: 9, mechanisms: "PLAIN".into(), locales: "en_US".into(), ..Default::default()
    }))).await;
    assert!(matches!(read(&mut stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::StartOk(_)))));
    send(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::Tune(connection::Tune { channel_max: 8, frame_max: 8192, heartbeat: 0 }))).await;
    assert!(matches!(read(&mut stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::TuneOk(_)))));
    assert!(matches!(read(&mut stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Open(_)))));
    send(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::OpenOk(connection::OpenOk {}))).await;
    stream
}
async fn finish(stream: &mut TcpStream) {
    assert!(matches!(read(stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(_)))));
    send(stream, 0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(connection::CloseOk {}))).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_connection_close_notifies_before_shutdown() {
    for reply_code in [200, 599] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (observer, mut seen) = make_observer();
        let address = listener.local_addr().unwrap();
        let (go, mut gate) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let mut stream = handshake(listener).await; gate.recv().await.unwrap();
            send(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close { reply_code, reply_text: "connection stop".into(), class_id: 0, method_id: 0 }))).await;
            assert!(matches!(read(&mut stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(_)))));
        });
        let connection = ends(Connection::connect_with_runtime(&format!("amqp://{address}/%2f"), ConnectionProperties::default().with_remote_close_observer(observer.clone()), Runtime::tokio_current())).await.unwrap();
        go.send(()).unwrap();
        let (close, accepted) = ends(seen.recv()).await.unwrap(); assert!(accepted);
        assert!(connection.status().connected());
        assert!(matches!(close, RemoteClose::Connection(ref c) if c.reply_code == reply_code && c.reply_text.as_str() == "connection stop" && c.class_id == 0 && c.method_id == 0));
        observer.release.store(true, Ordering::Release);
        ends(peer).await.unwrap();
        context(&connection.create_channel_completion().unwrap_err(), &observer.report);
        let _ = ends(tokio::task::spawn_blocking(move || connection.run())).await.unwrap();
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    }
}

async fn channel_case(reply_code: u16, pending: bool, before_return: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap(); let address = listener.local_addr().unwrap();
    let (observer, mut seen) = make_observer(); let (go, mut gate) = mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let mut stream = handshake(listener).await;
        assert!(matches!(read(&mut stream).await, AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::Open(_)))));
        send(&mut stream, 1, AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {}))).await;
        if pending { assert!(matches!(read(&mut stream).await, AMQPFrame::Method(1, AMQPClass::Queue(queue::AMQPMethod::Declare(_))))); }
        if !before_return { gate.recv().await.unwrap(); }
        send(&mut stream, 1, AMQPClass::Channel(channel::AMQPMethod::Close(channel::Close { reply_code, reply_text: "channel stop".into(), class_id: 50, method_id: 10 }))).await;
        assert!(matches!(read(&mut stream).await, AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::CloseOk(_)))));
        finish(&mut stream).await;
    });
    let connection = ends(Connection::connect_with_runtime(&format!("amqp://{address}/%2f"), ConnectionProperties::default(), Runtime::tokio_current())).await.unwrap();
    let completion = connection.create_channel_completion_with_close_observer(observer.clone()).unwrap();
    let channel = ends(completion).await.unwrap();
    let mut rpc = pending.then(|| channel.queue_declare_completion("q".into(), Default::default(), Default::default()).unwrap());
    if !before_return { go.send(()).unwrap(); }
    let (close, accepted) = ends(seen.recv()).await.unwrap(); assert!(accepted);
    assert!(channel.status().connected());
    if let Some(rpc) = &mut rpc { assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::pin::Pin::new(&mut *rpc).poll(cx).is_pending())).await); }
    assert!(matches!(close, RemoteClose::Channel(ref c) if c.reply_code == reply_code && c.reply_text.as_str() == "channel stop" && c.class_id == 50 && c.method_id == 10));
    observer.release.store(true, Ordering::Release);
    if let Some(rpc) = rpc { let error = ends(rpc).await.unwrap_err(); context(&error, &observer.report); }
    ends(async { while channel.status().connected() || channel.status().closing() { tokio::task::yield_now().await; } }).await;
    let error = channel.basic_qos_completion(1, Default::default()).unwrap_err(); context(&error, &observer.report);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    ends(connection.close(200, "done".into())).await.unwrap();
    ends(peer).await.unwrap();
    let _ = ends(tokio::task::spawn_blocking(move || connection.run())).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_channel_close_notifies_before_poison() { for code in [200, 599] { ends(channel_case(code, false, false)).await; } }
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_observer_is_installed_before_channel_open() { ends(channel_case(200, false, true)).await; }
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_rpc_keeps_the_observers_close_context() { for code in [200, 599, 406] { ends(channel_case(code, true, false)).await; } }
#[test]
#[should_panic(expected = "configure Close observer once")]
fn observer_configuration_is_immutable() { let (first, _) = make_observer(); let (second, _) = make_observer(); let _ = ConnectionProperties::default().with_remote_close_observer(first).with_remote_close_observer(second); }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_close_context_precedes_rejection_effects() {
    for rejected in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap(); let address = listener.local_addr().unwrap();
        let (observer, mut seen) = make_observer(); let (child_observer, _) = make_observer();
        child_observer.release.store(true, Ordering::Release);
        let (go, mut gate) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let mut stream = handshake(listener).await;
            assert!(matches!(read(&mut stream).await, AMQPFrame::Method(1, AMQPClass::Channel(channel::AMQPMethod::Open(_)))));
            send(&mut stream, 1, AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {}))).await;
            assert!(matches!(read(&mut stream).await, AMQPFrame::Method(1, AMQPClass::Queue(queue::AMQPMethod::Declare(_)))));
            gate.recv().await.unwrap();
            send(&mut stream, if rejected { 1 } else { 0 }, AMQPClass::Connection(connection::AMQPMethod::Close(connection::Close { reply_code: 200, reply_text: "connection stop".into(), class_id: 0, method_id: 0 }))).await;
            if rejected {
                let AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(close))) = read(&mut stream).await else { panic!("local protocol rejection") };
                assert_eq!(close.reply_code, 503);
                assert_eq!(close.reply_text.as_str(), "connection frame received on channel 1");
                send(&mut stream, 0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(connection::CloseOk {}))).await;
            } else { assert!(matches!(read(&mut stream).await, AMQPFrame::Method(0, AMQPClass::Connection(connection::AMQPMethod::CloseOk(_))))); }
        });
        let connection = ends(Connection::connect_with_runtime(&format!("amqp://{address}/%2f"), ConnectionProperties::default().with_remote_close_observer(observer.clone()), Runtime::tokio_current())).await.unwrap();
        let channel = ends(connection.create_channel_completion_with_close_observer(child_observer.clone()).unwrap()).await.unwrap();
        let mut pending = channel.queue_declare_completion("q".into(), Default::default(), Default::default()).unwrap();
        go.send(()).unwrap();
        let (method, accepted) = ends(seen.recv()).await.unwrap(); assert_eq!(accepted, !rejected);
        assert!(matches!(method, RemoteClose::Connection(_)));
        assert!(connection.status().connected(), "rejection mutated native state before observer returned");
        assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(std::pin::Pin::new(&mut pending).poll(cx).is_pending())).await); assert_eq!(child_observer.calls.load(Ordering::SeqCst), 0);
        observer.release.store(true, Ordering::Release);
        let error = ends(pending).await.unwrap_err(); context(&error, &observer.report);
        ends(peer).await.unwrap();
        context(&connection.create_channel_completion().unwrap_err(), &observer.report);
        let _ = ends(tokio::task::spawn_blocking(move || connection.run())).await.unwrap();
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    }
}
