use crate::Error;
use flume::{Receiver, Sender};
use futures_core::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Clone, Debug)]
// Wrap in an arc not to tamper with receiver count
pub(crate) struct Events(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    sender: Sender<Event>,
    receiver: Receiver<Event>,
    overflow: AtomicBool,
    rpc: Mutex<Option<crate::internal_rpc::InternalRPCHandle>>,
}

impl Events {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = flume::bounded(64);
        Self(Arc::new(Inner {
            sender,
            receiver,
            overflow: AtomicBool::new(false),
            rpc: Mutex::new(None),
        }))
    }

    pub(crate) fn bind(&self, rpc: crate::internal_rpc::InternalRPCHandle) {
        *self.0.rpc.lock().unwrap_or_else(|e| e.into_inner()) = Some(rpc);
    }

    pub(crate) fn sender(&self) -> EventsSender {
        EventsSender(self.0.clone())
    }

    pub(crate) fn listener(&self) -> impl Stream<Item = Event> + Send + 'static {
        EventStream {
            stream: self.0.receiver.clone().into_stream(),
            inner: self.0.clone(),
            reported: false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EventsSender(Arc<Inner>);
struct EventStream {
    stream: flume::r#async::RecvStream<'static, Event>,
    inner: Arc<Inner>,
    reported: bool,
}
impl Stream for EventStream {
    type Item = Event;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        if self.inner.overflow.load(Ordering::Acquire) && !self.reported {
            self.reported = true;
            return Poll::Ready(Some(Event::Error(
                crate::ErrorKind::ResourceLimitExceeded.into(),
            )));
        }
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl EventsSender {
    fn send(&self, event: Event) {
        // Do nothing if we don't have at least one external receiver
        if self.0.sender.receiver_count() > 1 {
            // The only possibility of error is if we have several external receivers and the
            // connection was already dropped, so we can safely ignore this.
            if self.0.sender.try_send(event).is_err()
                && !self.0.overflow.swap(true, Ordering::AcqRel)
                && let Some(rpc) = &*self.0.rpc.lock().unwrap_or_else(|e| e.into_inner())
            {
                rpc.set_connection_error(crate::ErrorKind::ResourceLimitExceeded.into());
            }
        }
    }

    pub(crate) fn connected(&self) {
        self.send(Event::Connected);
    }

    pub(crate) fn connection_blocked(&self, reason: String) {
        self.send(Event::ConnectionBlocked(reason));
    }

    pub(crate) fn connection_unblocked(&self) {
        self.send(Event::ConnectionUnblocked);
    }

    pub(crate) fn send_flow(&self, active: bool) {
        self.send(Event::SendFlow(active));
    }

    pub(crate) fn error(&self, error: Error) {
        self.send(Event::Error(error));
    }
}

/// A connection-level event delivered via [`Connection::events_listener`].
///
/// The stream produced by [`Connection::events_listener`] emits these values
/// as the connection progresses through its lifecycle.
///
/// [`Connection::events_listener`]: crate::Connection::events_listener
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    /// The connection has been established (or re-established after recovery).
    Connected,
    /// The broker has blocked the connection due to resource constraints.
    /// The inner string is the human-readable reason supplied by the broker.
    ConnectionBlocked(String),
    /// The broker has unblocked the connection.
    ConnectionUnblocked,
    /// The broker has changed the allowed flow direction.
    /// `true` means publishing is permitted; `false` means it is paused.
    SendFlow(bool),
    /// An error occurred on the connection.
    Error(Error),
}
