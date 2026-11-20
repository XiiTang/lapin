use crate::error::ReceivedClose;
use crate::{Error, Result, listener::Listener};
use std::{
    any::Any,
    cell::RefCell,
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

#[derive(Clone, Default)]
pub struct DispatchKey(Arc<()>);

impl DispatchKey {
    pub fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub(crate) struct ExpectedDispatch {
    pub key: DispatchKey,
    pub class: u16,
    pub method: u16,
    pub written: Arc<std::sync::atomic::AtomicBool>,
}

type DispatchObserver<T> = Box<dyn FnOnce(Result<T>, Option<Error>) + Send>;
type DispatchSink = (Option<DispatchKey>, Box<dyn Any>);

thread_local! {
    static DISPATCH_SINK: RefCell<Option<DispatchSink>> = RefCell::default();
}

struct DispatchScope(Option<DispatchSink>);
impl Drop for DispatchScope {
    fn drop(&mut self) {
        DISPATCH_SINK.with_borrow_mut(|slot| *slot = self.0.take());
    }
}

/// Install the observer at the promise behind one synchronous native
/// admission, before a frame or internal command can resolve it. A resolver
/// may run the observer during admission. The observer must not reenter lapin.
pub fn observe_dispatch<T: Send + 'static>(
    dispatch: impl FnOnce() -> Result<Completion<T>>,
    observer: impl FnOnce(Result<T>) + Send + 'static,
) {
    observe_dispatch_inner(None, dispatch, move |result, _| observer(result));
}

/// Bind a caller's exact request identity to the expected reply as well as its sink.
pub fn observe_dispatch_with_key<T: Send + 'static>(
    key: DispatchKey,
    dispatch: impl FnOnce() -> Result<Completion<T>>,
    observer: impl FnOnce(Result<T>) + Send + 'static,
) {
    observe_dispatch_inner(Some(key), dispatch, move |result, _| observer(result));
}

/// Observe a closing exchange. The second argument identifies a cancellation
/// implied by native channel retirement instead of a decoded CancelOk.
pub fn observe_claimed_dispatch<T: Send + 'static>(
    dispatch: impl FnOnce() -> Result<Completion<T>>,
    observer: impl FnOnce(Result<T>, Option<Error>) + Send + 'static,
) {
    observe_dispatch_inner(None, dispatch, observer);
}

fn observe_dispatch_inner<T: Send + 'static>(
    key: Option<DispatchKey>,
    dispatch: impl FnOnce() -> Result<Completion<T>>,
    observer: impl FnOnce(Result<T>, Option<Error>) + Send + 'static,
) {
    let observer: DispatchObserver<T> = Box::new(observer);
    let previous = DISPATCH_SINK.with_borrow_mut(|slot| slot.replace((key, Box::new(observer))));
    let _scope = DispatchScope(previous);
    let result = dispatch();
    let remaining = DISPATCH_SINK.with_borrow_mut(Option::take);
    if let Some((_, observer)) = remaining {
        let observer = observer
            .downcast::<DispatchObserver<T>>()
            .expect("the observer belongs to this native admission");
        match result {
            Ok(completion) => drop(completion.with_sink(move |result| observer(result, None))),
            Err(error) => observer(Err(error), None),
        }
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
        promise.shared.observe_dispatch();
        drop(self.with_sink(move |result| resolver.complete(result.map(map))));
        promise
    }

    /// Prebind a mapping that sits in front of native admission. The inner
    /// and outer receipts can have the same type, so claim the caller's sink
    /// on the outer receipt before the inner resolver can see it.
    pub(crate) fn map_dispatch<U>(
        marker: &str,
        dispatch: impl FnOnce() -> Result<Self>,
        map: impl FnOnce(T) -> U + Send + 'static,
    ) -> Result<Completion<U>>
    where
        T: Send + 'static,
        U: Send + 'static,
    {
        let observed = DISPATCH_SINK.with_borrow(|slot| {
            slot.as_ref().is_some_and(|(_, sink)| sink.is::<DispatchObserver<U>>())
        });
        if !observed {
            return Ok(dispatch()?.map(marker, map));
        }
        let (outer, resolver) = Completion::new(marker);
        outer.shared.observe_dispatch();
        observe_dispatch(dispatch, move |result| resolver.complete(result.map(map)));
        Ok(outer)
    }

    pub(crate) fn rpc(
        marker: &str,
        status: crate::ConnectionStatus,
        scope: &crate::task_scope::TaskScope,
        context: crate::frames::FrameErrorContext,
        channel: crate::types::ChannelId,
    ) -> (Self, PromiseResolver<T>)
    where
        T: Send + 'static,
    {
        let (promise, resolver) = Self::new(marker);
        let retirement: Arc<dyn crate::task_scope::RetiringReply> =
            Arc::new(ReplyRetirement(Arc::downgrade(&promise.shared)));
        {
            let mut state = promise.shared.lock_state();
            state.rejection = Some(status);
            state.rpc_context = Some((context, channel));
            state.retirement = Some(retirement.clone());
        }
        scope.register_reply(retirement);
        promise.shared.observe_dispatch();
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
    // A dropped RPC command still completes through the ordinary last-resolver
    // checkpoint, with its original error kind, text and completion ownership.
    pub(crate) fn retain_remote_close(&self, close: Option<Arc<ReceivedClose>>) {
        let mut state = self.shared.lock_state();
        if !state.completed && state.resolver_close.is_none() {
            state.resolver_close = close;
        }
    }

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

impl PromiseResolver<()> {
    pub(crate) fn settle_cancel(&self, cause: Error) {
        let action = {
            let mut state = self.shared.lock_state();
            if state.completed {
                return;
            }
            state.implied_cancel = Some(cause);
            state.complete(ReplyOutcome::Completed(Ok(())))
        };
        Shared::dispatch(action);
        self.shared.listener.notify();
    }
}

pub(crate) trait Cancelable {
    fn cancel(&self, err: Error);
    fn observe_dispatch(&self) {}
    fn expect_method(
        &self,
        _class: u16,
        _method: u16,
    ) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        None
    }
    fn expected_dispatch(&self) -> Option<ExpectedDispatch> {
        None
    }
}

impl<T: Send + 'static> Cancelable for PromiseResolver<T> {
    fn cancel(&self, err: Error) {
        self.reject(err)
    }

    fn observe_dispatch(&self) {
        self.shared.observe_dispatch();
    }

    fn expect_method(&self, class: u16, method: u16) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        let mut state = self.shared.lock_state();
        let key = state.dispatch_key.take()?;
        let written = Arc::new(std::sync::atomic::AtomicBool::new(false));
        state.expected_dispatch = Some(ExpectedDispatch {
            key,
            class,
            method,
            written: written.clone(),
        });
        Some(written)
    }

    fn expected_dispatch(&self) -> Option<ExpectedDispatch> {
        let state = self.shared.lock_state();
        (!state.completed)
            .then(|| state.expected_dispatch.clone())
            .flatten()
    }
}

impl<T: Send + 'static> Shared<T> {
    fn observe_dispatch(self: &Arc<Self>) {
        let observer = DISPATCH_SINK.with_borrow_mut(|slot| {
            if slot
                .as_ref()
                .is_some_and(|(_, sink)| sink.is::<DispatchObserver<T>>())
            {
                let (key, sink) = slot.take().expect("checked observer");
                Some((
                    key,
                    sink.downcast::<DispatchObserver<T>>()
                        .expect("checked observer type"),
                ))
            } else {
                None
            }
        });
        if let Some((key, observer)) = observer {
            let mut state = self.lock_state();
            state.dispatch_key = key;
            let data = state.data.take();
            state.closed = true;
            if let Some(data) = data {
                let implied = state.implied_cancel.take();
                drop(state);
                observer(data, implied);
            } else {
                let owner = Arc::downgrade(self);
                state.sink = Some(Box::new(move |result| {
                    let implied = owner
                        .upgrade()
                        .and_then(|owner| owner.lock_state().implied_cancel.take());
                    observer(result, implied);
                }));
            }
        }
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
    rpc_context: Option<(crate::frames::FrameErrorContext, crate::types::ChannelId)>,
    retirement: Option<Arc<dyn crate::task_scope::RetiringReply>>,
    resolver_close: Option<Arc<ReceivedClose>>,
    dispatch_key: Option<DispatchKey>,
    expected_dispatch: Option<ExpectedDispatch>,
    implied_cancel: Option<Error>,
}

type CompletionSink<T> = Box<dyn FnOnce(Result<T>) + Send>;
type CompletionAction<T> = Option<(Option<CompletionSink<T>>, Result<T>)>;

// Only resolver Drop and full scope retirement construct abandonment. A real
// completed I/O error always stays Completed, even when its kind is BrokenPipe.
enum ReplyOutcome<T> {
    Completed(Result<T>),
    Abandoned,
}

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
            return match outcome {
                ReplyOutcome::Completed(data) => Some((None, data)),
                ReplyOutcome::Abandoned => None,
            };
        }
        let data = match outcome {
            ReplyOutcome::Completed(data) => data,
            ReplyOutcome::Abandoned => Err(self
                .rejection
                .as_ref()
                .and_then(crate::ConnectionStatus::rejection)
                .unwrap_or_else(|| {
                    Error::from(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "promise lost its last resolver before completion",
                    ))
                    .with_remote_close(self.resolver_close.clone().or_else(
                        || {
                            self.rpc_context
                                .as_ref()
                                .and_then(|(context, channel)| context.remote_close(*channel))
                        },
                    ))
                })),
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
            rpc_context: None,
            retirement: None,
            resolver_close: None,
            dispatch_key: None,
            expected_dispatch: None,
            implied_cancel: None,
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
    fn claimed_dispatch_observes_settlement_before_admission_returns() {
        for implied in [false, true] {
            let (sent, received) = std::sync::mpsc::channel();
            observe_claimed_dispatch(
                || {
                    let (completion, resolver) = Completion::new("claimed Cancel");
                    resolver.observe_dispatch();
                    if implied {
                        resolver.settle_cancel(ErrorKind::MissingHeartbeatError.into());
                    } else {
                        resolver.resolve(());
                    }
                    let (result, cause) = received.try_recv().expect("settled at the resolver");
                    result.unwrap();
                    assert_eq!(cause.is_some(), implied);
                    Ok(completion)
                },
                move |result, cause| sent.send((result, cause)).unwrap(),
            );
            assert!(received.try_recv().is_err());
        }
    }

    #[test]
    fn same_type_mapped_dispatch_observes_the_projected_result() {
        let (sent, received) = std::sync::mpsc::channel();
        observe_dispatch(
            || Completion::map_dispatch(
                "projected value",
                || {
                    let (completion, resolver) = Completion::new("raw value");
                    resolver.observe_dispatch();
                    resolver.resolve(7);
                    assert_eq!(received.try_recv().unwrap().unwrap(), 8);
                    Ok(completion)
                },
                |value| value + 1,
            ),
            move |result| sent.send(result).unwrap(),
        );
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn recorded_close_survives_only_the_last_resolver_drop() {
        let close = Arc::new(ReceivedClose::new(
            crate::RemoteClose::Channel(crate::protocol::channel::Close {
                reply_code: 599,
                reply_text: "peer Close".into(),
                class_id: 0,
                method_id: 17,
            }),
            None,
        ));
        for abandoned in [false, true] {
            let (promise, resolver) = Completion::<()>::new("recorded Close");
            let other = resolver.clone();
            resolver.retain_remote_close(Some(close.clone()));
            let (send, receive) = std::sync::mpsc::channel();
            let promise = promise.with_sink(move |result| {
                send.send(result).unwrap();
            });
            drop(resolver);
            assert!(promise.try_wait().is_none());
            if abandoned {
                drop(promise);
                drop(other);
                let error = receive.try_recv().unwrap().unwrap_err();
                assert_eq!(error.remote_close(), Some(close.method()));
                assert!(receive.try_recv().is_err());
            } else {
                drop(other);
                let error = promise.try_wait().unwrap().unwrap_err();
                assert_eq!(error.remote_close(), Some(close.method()));
                assert!(
                    matches!(error.kind(), ErrorKind::IOError(cause) if cause.kind() == std::io::ErrorKind::BrokenPipe)
                );
            }
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
    fn taken_completion_keeps_the_answer_and_drop_routes_pending_answers() {
        for ready in [false, true] {
            let (promise, resolver) = Completion::new("take or drop");
            let (send, recv) = std::sync::mpsc::channel();
            let promise = promise.with_sink(move |result| send.send(result).unwrap());
            if ready {
                resolver.resolve(7);
                assert!(matches!(futures_lite::future::block_on(promise), Ok(7)));
            } else {
                drop(promise);
                resolver.resolve(7);
                assert!(matches!(recv.try_recv(), Ok(Ok(7))));
            }
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
