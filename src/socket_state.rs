use crate::Result;
use flume::{Receiver, Sender};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Poll, Wake, Waker},
};
use tracing::{error, trace};

pub(crate) struct SocketState {
    readable: bool,
    writable: bool,
    events: Receiver<()>,
    handle: SocketStateHandle,
}

impl Default for SocketState {
    fn default() -> Self {
        let (sender, receiver) = flume::bounded(1);
        Self {
            readable: true,
            writable: true,
            events: receiver,
            handle: SocketStateHandle {
                sender,
                pending: Arc::new(AtomicU8::new(0)),
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct SocketStateHandle {
    sender: Sender<()>,
    pending: Arc<AtomicU8>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SocketEvent {
    Readable,
    Writable,
    Wake,
}

pub(crate) struct SocketStateWaker {
    handle: SocketStateHandle,
    event: SocketEvent,
}

impl SocketState {
    pub(crate) fn readable(&mut self) -> bool {
        self.readable
    }

    pub(crate) fn writable(&mut self) -> bool {
        self.writable
    }

    pub(crate) fn wait(&mut self) {
        match self.events.recv() {
            Ok(()) => self.apply_pending(),
            Err(err) => error!(?err, "waiting for socket event failed"),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.readable = true;
        self.writable = true;
    }

    pub(crate) fn handle(&self) -> SocketStateHandle {
        self.handle.clone()
    }

    pub(crate) fn handle_read_poll(&mut self, poll: Poll<usize>) -> Option<usize> {
        match poll {
            Poll::Ready(sz) => Some(sz),
            Poll::Pending => {
                self.readable = false;
                None
            }
        }
    }

    pub(crate) fn handle_write_poll<T>(&mut self, poll: Poll<T>) -> Option<T> {
        match poll {
            Poll::Ready(sz) => Some(sz),
            Poll::Pending => {
                self.writable = false;
                None
            }
        }
    }

    pub(crate) fn handle_io_result(&mut self, result: Result<()>) -> Result<()> {
        if let Err(err) = result {
            if err.interrupted() {
                self.handle.wake();
            } else if err.wouldblock() {
                // ignore, let the reactor handle that
            } else {
                if err.is_io_error() {
                    self.readable = false;
                    self.writable = false;
                }
                return Err(err);
            }
        }
        Ok(())
    }

    pub(crate) fn poll_events(&mut self) {
        if self.events.try_recv().is_ok() {
            self.apply_pending();
        }
    }

    fn apply_pending(&mut self) {
        let pending = self.handle.pending.swap(0, Ordering::AcqRel);
        if pending & 1 != 0 {
            self.handle_event(SocketEvent::Readable);
        }
        if pending & 2 != 0 {
            self.handle_event(SocketEvent::Writable);
        }
    }

    fn handle_event(&mut self, event: SocketEvent) {
        trace!(?event, "Got event for socket");
        match event {
            SocketEvent::Readable => self.readable = true,
            SocketEvent::Writable => self.writable = true,
            SocketEvent::Wake => {}
        }
    }

    pub(crate) fn readable_waker(&self) -> Waker {
        self.waker(SocketEvent::Readable)
    }

    pub(crate) fn writable_waker(&self) -> Waker {
        self.waker(SocketEvent::Writable)
    }

    fn waker(&self, event: SocketEvent) -> Waker {
        let handle = self.handle();
        let waker = SocketStateWaker { handle, event };
        Waker::from(Arc::new(waker))
    }
}

impl SocketStateHandle {
    pub(crate) fn send(&self, event: SocketEvent) {
        let bit = match event {
            SocketEvent::Readable => 1,
            SocketEvent::Writable => 2,
            SocketEvent::Wake => 4,
        };
        self.pending.fetch_or(bit, Ordering::Release);
        // Coalesce notifications, preserving read/write readiness separately.
        // Never block a reactor or grow a queue when physical I/O stalls.
        let _ = self.sender.try_send(());
    }

    pub(crate) fn wake(&self) {
        self.send(SocketEvent::Wake);
    }
}

impl Wake for SocketStateWaker {
    fn wake(self: Arc<Self>) {
        self.handle.send(self.event)
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.handle.send(self.event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wake_storm_is_bounded_and_preserves_both_readiness_flags() {
        let mut state = SocketState::default();
        state.readable = false;
        state.writable = false;
        let handle = state.handle();
        std::thread::scope(|scope| {
            for event in [
                SocketEvent::Readable,
                SocketEvent::Writable,
                SocketEvent::Wake,
            ] {
                let handle = handle.clone();
                scope.spawn(move || {
                    for _ in 0..10000 {
                        handle.send(event);
                    }
                });
            }
        });
        assert_eq!(state.events.len(), 1);
        state.wait();
        assert!(state.readable && state.writable);
        assert_eq!(state.events.len(), 0);
        state.readable = false;
        handle.send(SocketEvent::Readable);
        state.poll_events();
        assert!(state.readable);
    }
}
