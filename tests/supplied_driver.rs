#![cfg(feature = "tokio")]
use async_rs::Runtime;
use futures_io::{AsyncRead, AsyncWrite};
use lapin::{Connection, ConnectionProperties, auth::AuthProvider};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

struct External;
impl AuthProvider for External {}

struct Probe {
    dropped: Arc<AtomicBool>,
    writes: Arc<AtomicUsize>,
    mode: Mode,
}
#[derive(Clone, Copy)]
enum Mode {
    Pending,
    Eof,
    Write,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl AsyncRead for Probe {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.mode {
            Mode::Eof => Poll::Ready(Ok(0)),
            _ => Poll::Pending,
        }
    }
}
impl AsyncWrite for Probe {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::Write => Poll::Ready(Ok(bytes.len())),
            _ => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        panic!("owner must not implicitly close supplied transport")
    }
}
fn probe(mode: Mode) -> (Probe, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    (
        Probe {
            dropped: dropped.clone(),
            writes: writes.clone(),
            mode,
        },
        dropped,
        writes,
    )
}
async fn until(mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stopped_before_handshake_poll_releases_transport() {
    let (stream, dropped, writes) = probe(Mode::Pending);
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        stream,
        ConnectionProperties::default().with_auth_provider(External),
    )
    .unwrap();
    owner.stop();
    drop(handshake);
    until(|| owner.is_finished()).await;
    tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn blocked_header_write_can_be_canceled_and_joined() {
    let (stream, dropped, writes) = probe(Mode::Pending);
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        stream,
        ConnectionProperties::default().with_auth_provider(External),
    )
    .unwrap();
    let handshake = tokio::spawn(handshake);
    until(|| writes.load(Ordering::SeqCst) != 0).await;
    owner.stop();
    until(|| owner.is_finished()).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handshake)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropping_owner_stops_pending_server_reply() {
    let (stream, dropped, writes) = probe(Mode::Write);
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        stream,
        ConnectionProperties::default().with_auth_provider(External),
    )
    .unwrap();
    let handshake = tokio::spawn(handshake);
    until(|| writes.load(Ordering::SeqCst) != 0).await;
    drop(owner);
    until(|| dropped.load(Ordering::SeqCst)).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handshake)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn eof_does_not_retry_supplied_transport() {
    let (stream, dropped, _) = probe(Mode::Eof);
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        stream,
        ConnectionProperties::default().with_auth_provider(External),
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handshake)
            .await
            .unwrap()
            .is_err()
    );
    until(|| owner.is_finished()).await;
    let _ = tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn rejects_implicit_authentication_and_recovery_before_io() {
    for options in [
        ConnectionProperties::default(),
        ConnectionProperties::default()
            .with_auth_provider(External)
            .enable_auto_recover(),
    ] {
        let (stream, dropped, writes) = probe(Mode::Write);
        assert!(
            Connection::from_stream(
                "amqp://invalid.invalid/%2f".parse().unwrap(),
                Runtime::tokio_current(),
                stream,
                options
            )
            .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }
}

struct StartStream {
    bytes: io::Cursor<Vec<u8>>,
    header_sent: bool,
    read_waker: Option<std::task::Waker>,
    eof_after_start: bool,
}
impl StartStream {
    fn new() -> Self {
        use amq_protocol::{
            frame::{AMQPFrame, gen_frame},
            protocol::{AMQPClass, connection},
        };
        let frame = AMQPFrame::Method(
            0,
            AMQPClass::Connection(connection::AMQPMethod::Start(connection::Start {
                version_major: 0,
                version_minor: 9,
                mechanisms: "EXTERNAL".into(),
                locales: "en_US".into(),
                ..Default::default()
            })),
        );
        Self {
            bytes: io::Cursor::new(gen_frame(&frame)(Vec::new().into()).unwrap().into_inner().0),
            header_sent: false,
            read_waker: None,
            eof_after_start: false,
        }
    }
}
impl AsyncRead for StartStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.read_waker = Some(cx.waker().clone());
        if !self.header_sent {
            return Poll::Pending;
        }
        if self.bytes.position() == self.bytes.get_ref().len() as u64 {
            return if self.eof_after_start {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            };
        }
        Poll::Ready(io::Read::read(&mut self.bytes, bytes))
    }
}
impl AsyncWrite for StartStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.header_sent = true;
        if let Some(waker) = self.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
struct PendingAuth {
    entered: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}
impl AuthProvider for PendingAuth {
    fn auth_starter(&self) -> Result<lapin::types::LongString, String> {
        panic!("async provider must be used")
    }
    fn auth_starter_async(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<lapin::types::LongString, String>> + Send + '_>,
    > {
        Box::pin(async move {
            struct OnDrop(Arc<AtomicBool>);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _guard = OnDrop(self.dropped.clone());
            self.entered.store(true, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn canceled_async_authentication_rejects_handshake_and_drains_resources() {
    let entered = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let options = ConnectionProperties::default().with_auth_provider(PendingAuth {
        entered: entered.clone(),
        dropped: dropped.clone(),
    });
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        StartStream::new(),
        options,
    )
    .unwrap();
    let handshake = tokio::spawn(handshake);
    until(|| entered.load(Ordering::SeqCst)).await;
    owner.stop();
    until(|| owner.is_finished()).await;
    assert!(dropped.load(Ordering::SeqCst));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handshake)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap()
        .unwrap();
}
struct FailedAuth;
impl AuthProvider for FailedAuth {
    fn auth_starter(&self) -> Result<lapin::types::LongString, String> {
        Err("fixture authentication failed".into())
    }
}
#[tokio::test]
async fn authentication_failure_resolves_waiter_without_waiting_for_server() {
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        StartStream::new(),
        ConnectionProperties::default().with_auth_provider(FailedAuth),
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), handshake)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("fixture authentication failed"));
    owner.stop();
    until(|| owner.is_finished()).await;
    let _ = tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap();
}

#[tokio::test]
async fn eof_during_authentication_ends_handshake_without_explicit_stop() {
    let entered = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let mut stream = StartStream::new();
    stream.eof_after_start = true;
    let (owner, handshake) = Connection::from_stream(
        "amqp://invalid.invalid/%2f".parse().unwrap(),
        Runtime::tokio_current(),
        stream,
        ConnectionProperties::default().with_auth_provider(PendingAuth { entered, dropped }),
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handshake)
            .await
            .unwrap()
            .is_err()
    );
    until(|| owner.is_finished()).await;
    let _ = tokio::task::spawn_blocking(move || owner.join())
        .await
        .unwrap();
}
