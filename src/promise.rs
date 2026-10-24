use crate::{Error, Result, listener::Listener};
use std::{
    fmt,
    future::Future,
    panic::RefUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
};
use tracing::{Level, level_enabled, trace};

/// The native completion receiver of an admitted operation.
///
/// Install a sink before giving this handle to a bounded waiter. Dropping the
/// handle closes then takes: an unread result, or a later completion, goes to
/// the sink exactly once. Taking a result transfers ownership to the caller.
#[must_use = "Completion should be awaited or handed to its owner"]
pub struct Completion<T> {
    shared: Arc<Shared<T>>,
    listener: Listener,
}

impl<T> fmt::Debug for Completion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Completion")
    }
}

impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        trace!(
            promise = %self.shared.marker(),
            "Dropping promise.",
        );
        self.shared.abandon();
    }
}

impl<T> Completion<T> {
    /// Own any result this receiver does not take, including errors and loss of
    /// the last resolver. The sink runs synchronously, outside the completion
    /// mutex, on the completing or dropping thread. It must not block or call
    /// back into lapin; hand the value to your recovery owner instead.
    pub fn with_sink(self, sink: impl FnOnce(Result<T>) + Send + 'static) -> Self {
        let previous = self.shared.lock_state().sink.replace(Box::new(sink));
        drop(previous);
        self
    }

    /// Close the receiver and take an already committed result atomically.
    /// A result arriving later goes to the installed sink. Do not poll again.
    #[must_use]
    pub fn close_and_take(&self) -> Option<Result<T>> {
        let (data, sink) = {
            let mut state = self.shared.lock_state();
            state.closed = true;
            let data = state.data.take();
            let sink = data.as_ref().and_then(|_| state.sink.take());
            (data, sink)
        };
        drop(sink);
        data
    }

    /// Take a completed result without closing a still-pending receiver.
    #[must_use]
    pub fn try_take(&self) -> Option<Result<T>> {
        self.shared.take()
    }

    pub(crate) fn map<U>(
        self,
        marker: &str,
        map: impl FnOnce(T) -> U + Send + 'static,
    ) -> Completion<U>
    where
        T: Send + 'static,
        U: Send + 'static,
    {
        let (promise, resolver) = Completion::new(marker);
        drop(self.with_sink(move |result| resolver.complete(result.map(map))));
        promise
    }

    pub(crate) fn rpc(marker: &str, status: crate::ConnectionStatus, scope: &crate::task_scope::TaskScope) -> (Self, PromiseResolver<T>)
    where T: Send + 'static {
        let (promise, resolver) = Self::new(marker);
        let retirement: Arc<dyn crate::task_scope::RetiringReply> = Arc::new(ReplyRetirement(Arc::downgrade(&promise.shared)));
        {
            let mut state = promise.shared.lock_state();
            state.rejection = Some(status);
            state.retirement = Some(retirement.clone());
        }
        scope.register_reply(retirement);
        (promise, resolver)
    }

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

impl<T> Future for Completion<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            self.listener.arm();
            if let Some(data) = self.shared.take() {
                self.listener.disarm();
                return Poll::Ready(data);
            }
            let state = self.shared.lock_state();
            if state.closed || (state.completed && state.data.is_none()) {
                drop(state);
                self.listener.disarm();
                return Poll::Ready(Err(crate::ErrorKind::FutureCompleted.into()));
            }
            drop(state);
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
            let action = state.complete(ReplyOutcome::Abandoned);
            drop(state);
            Shared::dispatch(action);
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
    closed: bool,
    sink: Option<CompletionSink<T>>,
    rejection: Option<crate::ConnectionStatus>,
    retirement: Option<Arc<dyn crate::task_scope::RetiringReply>>,
}

type CompletionSink<T> = Box<dyn FnOnce(Result<T>) + Send>;
type CompletionAction<T> = Option<(Option<CompletionSink<T>>, Result<T>)>;

// Only resolver Drop and full scope retirement construct abandonment. A real
// completed I/O error always stays Completed, even when its kind is BrokenPipe.
enum ReplyOutcome<T> { Completed(Result<T>), Abandoned }

struct ReplyRetirement<T>(Weak<Shared<T>>);
impl<T: Send + 'static> crate::task_scope::RetiringReply for ReplyRetirement<T> {
    fn retire(&self) {
        if let Some(shared) = self.0.upgrade() {
            let action = shared.lock_state().complete(ReplyOutcome::Abandoned);
            Shared::dispatch(action);
            shared.listener.notify();
        }
    }
}

impl<T> State<T> {
    fn complete(&mut self, outcome: ReplyOutcome<T>) -> CompletionAction<T> {
        if self.completed {
            // A late result is dropped outside the mutex by the existing dispatcher.
            return match outcome { ReplyOutcome::Completed(data) => Some((None, data)), ReplyOutcome::Abandoned => None };
        }
        let data = match outcome {
            ReplyOutcome::Completed(data) => data,
            ReplyOutcome::Abandoned => Err(self.rejection.as_ref().and_then(crate::ConnectionStatus::rejection).unwrap_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "promise lost its last resolver before completion").into())),
        };
        self.completed = true;
        self.retirement.take();
        if self.closed {
            Some((self.sink.take(), data))
        } else {
            self.data = Some(data);
            None
        }
    }
}

// Listener wraps event-listener::Event which uses UnsafeCell internally, opting
// out of RefUnwindSafe by default. Completion and sink ownership are committed
// before notify/dispatch run, outside the mutex. A panicking waker or sink cannot
// make the state incomplete again or invoke the sink a second time.
impl<T> RefUnwindSafe for Shared<T> where Result<T>: RefUnwindSafe {}

impl<T> Shared<T> {
    fn new(data: Option<Result<T>>, marker: &str) -> Arc<Self> {
        let mut state = State {
            completed: false,
            data: None,
            resolvers: 0,
            closed: false,
            sink: None,
            rejection: None,
            retirement: None,
        };
        if let Some(data) = data {
            // A ready receiver uses the same once-only completion checkpoint.
            Self::dispatch(state.complete(ReplyOutcome::Completed(data)));
        }
        Arc::new(Self {
            state: Mutex::new(state),
            listener: Listener::default(),
            marker: if level_enabled!(Level::TRACE) {
                Some(marker.into())
            } else {
                None
            },
        })
    }

    fn set(&self, data: Result<T>) {
        let action = self.lock_state().complete(ReplyOutcome::Completed(data));
        Self::dispatch(action);
        self.listener.notify();
    }

    fn take(&self) -> Option<Result<T>> {
        let (data, sink) = {
            let mut state = self.lock_state();
            let data = state.data.take();
            let sink = data.as_ref().and_then(|_| state.sink.take());
            (data, sink)
        };
        drop(sink);
        data
    }

    fn abandon(&self) {
        let action = {
            let mut state = self.lock_state();
            state.closed = true;
            state.data.take().map(|data| (state.sink.take(), data))
        };
        Self::dispatch(action);
    }

    fn dispatch(action: CompletionAction<T>) {
        if let Some((Some(sink), data)) = action {
            sink(data);
        }
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
        let (mut promise, resolver) = Completion::<()>::new("abandoned");
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
        let (promise, resolver) = Completion::new("completed");
        resolver.resolve(7);
        assert!(matches!(promise.try_wait(), Some(Ok(7))));
        resolver.reject(ErrorKind::MissingHeartbeatError.into());
        assert!(promise.try_wait().is_none());
        drop(resolver);
        assert!(promise.try_wait().is_none());
    }

    #[test]
    fn a_known_cause_wins_over_resolver_drop() {
        let (promise, resolver) = Completion::<()>::new("known error");
        let other = resolver.clone();
        resolver.reject(ErrorKind::MissingHeartbeatError.into());
        drop(resolver);
        drop(other);
        assert!(
            matches!(promise.try_wait(), Some(Err(error)) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
        );
        assert!(promise.try_wait().is_none());
    }

    #[test]
    fn completion_sink_owns_both_sides_of_close_and_runs_outside_the_mutex() {
        for ready in [false, true] {
            let (promise, resolver) = Completion::new("sink");
            let shared = Arc::downgrade(&promise.shared);
            let (send, recv) = std::sync::mpsc::channel();
            let promise = promise.with_sink(move |result| {
                assert!(shared.upgrade().unwrap().state.try_lock().is_ok());
                send.send(result).unwrap();
            });
            if ready {
                resolver.resolve(7);
            }
            drop(promise);
            if !ready {
                resolver.resolve(7);
            }
            assert!(matches!(recv.try_recv(), Ok(Ok(7))));
            resolver.resolve(8);
            resolver.reject(ErrorKind::MissingHeartbeatError.into());
            drop(resolver);
            assert!(recv.try_recv().is_err());
        }
    }

    #[test]
    fn close_then_take_keeps_the_committed_answer_and_routes_only_late_answers() {
        for ready in [false, true] {
            let (promise, resolver) = Completion::new("close then take");
            let (send, recv) = std::sync::mpsc::channel();
            let promise = promise.with_sink(move |result| send.send(result).unwrap());
            if ready {
                resolver.resolve(7);
                assert!(matches!(promise.close_and_take(), Some(Ok(7))));
            } else {
                assert!(promise.close_and_take().is_none());
                resolver.resolve(7);
                assert!(matches!(recv.try_recv(), Ok(Ok(7))));
            }
            drop(promise);
            drop(resolver);
            assert!(recv.try_recv().is_err());
        }
    }

    #[test]
    fn sink_preserves_known_errors_and_abandoned_resolver_completion_once() {
        for known in [false, true] {
            let (promise, resolver) = Completion::<()>::new("sink errors");
            let other = resolver.clone();
            let (send, recv) = std::sync::mpsc::channel();
            drop(promise.with_sink(move |result| send.send(result).unwrap()));
            if known {
                resolver.reject(ErrorKind::MissingHeartbeatError.into());
            }
            drop(resolver);
            drop(other);
            let error = recv.try_recv().unwrap().unwrap_err();
            if known {
                assert!(matches!(error.kind(), ErrorKind::MissingHeartbeatError));
            } else {
                assert!(
                    matches!(error.kind(), ErrorKind::IOError(cause) if cause.kind() == std::io::ErrorKind::BrokenPipe)
                );
            }
            assert!(recv.try_recv().is_err());
        }
    }

    #[test]
    fn racing_drop_and_completion_transfer_one_result() {
        for _ in 0..32 {
            let (promise, resolver) = Completion::new("concurrent sink");
            let (send, recv) = std::sync::mpsc::channel();
            let promise = promise.with_sink(move |result| send.send(result).unwrap());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let sender_barrier = barrier.clone();
            let sender = std::thread::spawn(move || {
                sender_barrier.wait();
                resolver.resolve(7);
            });
            barrier.wait();
            drop(promise);
            sender.join().unwrap();
            assert!(matches!(recv.try_recv(), Ok(Ok(7))));
            assert!(recv.try_recv().is_err());
        }
    }
}
