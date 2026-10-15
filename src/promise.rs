use crate::{Error, Result, listener::Listener};
use std::{
    fmt,
    future::Future,
    panic::RefUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};
use tracing::{Level, level_enabled, trace};

#[must_use = "Promise should be used or you can miss errors"]
pub(crate) struct Promise<T> {
    shared: Arc<Shared<T>>,
    listener: Listener,
}

impl<T> fmt::Debug for Promise<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Promise")
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        trace!(
            promise = %self.shared.marker(),
            "Dropping promise.",
        );
    }
}

impl<T> Promise<T> {
    pub(crate) fn new(marker: &str) -> (Self, PromiseResolver<T>) {
        let promise = Self::build(marker, None);
        let resolver = promise.resolver();
        (promise, resolver)
    }

    pub(crate) fn new_with_data(marker: &str, data: Result<T>) -> Self {
        Self::build(marker, Some(data))
    }

    fn build(marker: &str, data: Option<Result<T>>) -> Self {
        let shared = Shared::new(data, marker);
        let listener = shared.listener.clone();
        Self { shared, listener }
    }

    pub(crate) fn try_wait(&self) -> Option<Result<T>> {
        self.shared.take()
    }

    fn resolver(&self) -> PromiseResolver<T> {
        self.shared.lock_state().resolvers += 1;
        PromiseResolver {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Future for Promise<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            self.listener.arm();
            if let Some(data) = self.shared.take() {
                self.listener.disarm();
                return Poll::Ready(data);
            }
            match self.listener.poll(cx) {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub(crate) struct PromiseResolver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> fmt::Debug for PromiseResolver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromiseResolver")
    }
}

impl<T> Clone for PromiseResolver<T> {
    fn clone(&self) -> Self {
        self.shared.lock_state().resolvers += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for PromiseResolver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.lock_state();
        state.resolvers -= 1;
        if state.resolvers == 0 && !state.completed {
            state.completed = true;
            state.data = Some(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "promise lost its last resolver before completion",
            )
            .into()));
            drop(state);
            self.shared.listener.notify();
        }
    }
}

impl<T> PromiseResolver<T> {
    pub(crate) fn resolve(&self, data: T) {
        self.complete(Ok(data))
    }

    pub(crate) fn reject(&self, error: Error) {
        self.complete(Err(error))
    }

    pub(crate) fn complete(&self, res: Result<T>) {
        trace!(
            promise = %self.shared.marker(),
            "Resolving promise.",
        );
        self.shared.set(res);
    }
}

pub(crate) trait Cancelable {
    fn cancel(&self, err: Error);
}

impl<T> Cancelable for PromiseResolver<T> {
    fn cancel(&self, err: Error) {
        self.reject(err)
    }
}

struct Shared<T> {
    state: Mutex<State<T>>,
    listener: Listener,
    marker: Option<String>,
}

struct State<T> {
    data: Option<Result<T>>,
    // Taking the result does not allow another resolver to complete it again.
    completed: bool,
    // Count only resolver handles: the promise also holds an Arc to Shared.
    resolvers: usize,
}

// Listener wraps event-listener::Event which uses UnsafeCell internally, opting
// out of RefUnwindSafe by default. The only panic vector is Waker::wake() inside
// notify(); if it panics the waiting task is not woken, but the data is already
// written before notify() is called so a subsequent poll will find it.
impl<T> RefUnwindSafe for Shared<T> where Result<T>: RefUnwindSafe {}

impl<T> Shared<T> {
    fn new(data: Option<Result<T>>, marker: &str) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                completed: data.is_some(),
                data,
                resolvers: 0,
            }),
            listener: Listener::default(),
            marker: if level_enabled!(Level::TRACE) {
                Some(marker.into())
            } else {
                None
            },
        })
    }

    fn set(&self, data: Result<T>) {
        let mut lock = self.lock_state();
        if !lock.completed {
            lock.completed = true;
            lock.data = Some(data);
            // Release the lock before waking to avoid the woken task
            // immediately blocking on it.
            drop(lock);
            self.listener.notify();
        }
    }

    fn take(&self) -> Option<Result<T>> {
        self.lock_state().data.take()
    }

    fn lock_state(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn marker(&self) -> String {
        self.marker
            .as_ref()
            .map_or(String::default(), |marker| format!("[{marker}] "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Wake, Waker},
    };

    #[derive(Default)]
    struct Woken(AtomicUsize);
    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn last_resolver_drop_rejects_and_wakes_the_waiter() {
        let (mut promise, resolver) = Promise::<()>::new("abandoned");
        let other = resolver.clone();
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(woken.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut promise).poll(&mut cx).is_pending());
        drop(resolver);
        assert_eq!(woken.0.load(Ordering::SeqCst), 0);
        assert!(promise.try_wait().is_none());
        drop(other);
        assert_eq!(woken.0.load(Ordering::SeqCst), 1);
        assert!(
            matches!(promise.try_wait(), Some(Err(error)) if matches!(error.kind(), ErrorKind::IOError(cause) if cause.kind() == std::io::ErrorKind::BrokenPipe))
        );
    }

    #[test]
    fn completion_is_permanent_after_the_result_is_taken() {
        let (promise, resolver) = Promise::new("completed");
        resolver.resolve(7);
        assert!(matches!(promise.try_wait(), Some(Ok(7))));
        resolver.reject(ErrorKind::MissingHeartbeatError.into());
        assert!(promise.try_wait().is_none());
        drop(resolver);
        assert!(promise.try_wait().is_none());
    }

    #[test]
    fn a_known_cause_wins_over_resolver_drop() {
        let (promise, resolver) = Promise::<()>::new("known error");
        let other = resolver.clone();
        resolver.reject(ErrorKind::MissingHeartbeatError.into());
        drop(resolver);
        drop(other);
        assert!(
            matches!(promise.try_wait(), Some(Err(error)) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
        );
        assert!(promise.try_wait().is_none());
    }
}
