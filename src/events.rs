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

const EVENT_QUEUE_MESSAGES: usize = 64;
fn overflow_error() -> Error {
    crate::ResourceLimit::EventQueueMessages
        .exceeded(EVENT_QUEUE_MESSAGES, Some(EVENT_QUEUE_MESSAGES as u64 + 1))
}

#[derive(Clone, Debug)]
// Wrap in an arc not to tamper with receiver count
pub(crate) struct Events(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    sender: Sender<Event>,
    receiver: Receiver<Event>,
    overflow: AtomicBool,
    /// One event that found the queue full. While it is held the I/O loop
    /// reads and parses nothing, so nothing else from the peer arrives;
    /// a dequeue moves it in and wakes the loop.
    held: Mutex<Option<Event>>,
    rpc: Mutex<Option<crate::internal_rpc::InternalRPCHandle>>,
}

impl Events {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = flume::bounded(EVENT_QUEUE_MESSAGES);
        Self(Arc::new(Inner {
            sender,
            receiver,
            overflow: AtomicBool::new(false),
            held: Mutex::new(None),
            rpc: Mutex::new(None),
        }))
    }

    pub(crate) fn bind(&self, rpc: crate::internal_rpc::InternalRPCHandle) {
        *self.0.rpc.lock().unwrap_or_else(|e| e.into_inner()) = Some(rpc);
    }

    /// Whether an event waits for room, so the I/O loop must not read.
    pub(crate) fn held(&self) -> bool {
        self.0
            .held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
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
            return Poll::Ready(Some(Event::Error(overflow_error())));
        }
        let next = Pin::new(&mut self.stream).poll_next(cx);
        if let Poll::Ready(Some(_)) = &next {
            self.inner.release_held();
        }
        next
    }
}

impl Inner {
    /// Moves the held event into the room a dequeue made, then wakes the
    /// I/O loop to read again.
    fn release_held(&self) {
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        let Some(event) = held.take() else {
            return;
        };
        if let Err(flume::TrySendError::Full(event) | flume::TrySendError::Disconnected(event)) =
            self.sender.try_send(event)
        {
            *held = Some(event);
            return;
        }
        drop(held);
        if let Some(rpc) = &*self.rpc.lock().unwrap_or_else(|e| e.into_inner()) {
            rpc.wake();
        }
    }

    fn overflow(&self) {
        if !self.overflow.swap(true, Ordering::AcqRel)
            && let Some(rpc) = &*self.rpc.lock().unwrap_or_else(|e| e.into_inner())
        {
            rpc.set_connection_error(overflow_error());
        }
    }
}

impl EventsSender {
    fn send(&self, event: Event) {
        // Do nothing if we don't have at least one external receiver
        if self.0.sender.receiver_count() > 1 {
            // A full queue holds one event behind those queued, and the I/O
            // loop reads nothing more until it is in. An Error ends the
            // connection, so it is never held: it overflows as before.
            // Behind a held event the queue is full: what arrives then comes
            // from outside the paused loop and overflows, as before.
            let mut held = self.0.held.lock().unwrap_or_else(|e| e.into_inner());
            if held.is_none() {
                match self.0.sender.try_send(event) {
                    Ok(()) => return,
                    Err(flume::TrySendError::Full(event)) if !matches!(event, Event::Error(_)) => {
                        *held = Some(event);
                        return;
                    }
                    Err(_) => {}
                }
            }
            drop(held);
            self.0.overflow();
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn a_full_event_queue_holds_one_event_and_overflows_only_an_error() {
        let events = Events::new();
        let mut listener = Box::pin(events.listener());
        for _ in 0..=EVENT_QUEUE_MESSAGES {
            events.sender().connection_unblocked();
        }
        assert!(events.held());
        // The held event is no overflow: the queued ones come first, then it.
        for _ in 0..=EVENT_QUEUE_MESSAGES {
            assert!(matches!(
                futures_lite::future::poll_once(futures_lite::StreamExt::next(&mut listener)).await,
                Some(Some(Event::ConnectionUnblocked))
            ));
        }
        assert!(!events.held());
        for _ in 0..EVENT_QUEUE_MESSAGES {
            events.sender().connection_unblocked();
        }
        events
            .sender()
            .error(crate::ErrorKind::MissingHeartbeatError.into());
        assert!(!events.held());
        let Event::Error(error) =
            futures_lite::future::poll_once(futures_lite::StreamExt::next(&mut listener))
                .await
                .expect("the full queue must report without waiting")
                .unwrap()
        else {
            panic!("overflow must be reported before the retained events");
        };
        assert!(
            matches!(
                error.kind(),
                crate::ErrorKind::ResourceLimitExceeded {
                    limit: crate::ResourceLimit::EventQueueMessages,
                    maximum: 64,
                    observed: Some(65),
                }
            ),
            "{error}"
        );
    }
}
