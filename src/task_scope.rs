use event_listener::{Event, EventListener};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
};

#[derive(Clone, Default)]
pub(crate) struct TaskScope(Arc<Inner>);
#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    drained: Condvar,
    stopped: Event,
}
#[derive(Default)]
struct State {
    stopped: bool,
    active: usize,
}
impl TaskScope {
    pub(crate) fn wrap(&self, future: impl Future<Output = ()> + Send + 'static) -> ScopedFuture {
        let listener = Box::pin(self.0.stopped.listen());
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped {
            drop(state);
            drop(future);
            return ScopedFuture {
                future: None,
                listener,
                guard: None,
            };
        }
        state.active += 1;
        ScopedFuture {
            future: Some(Box::pin(future)),
            listener,
            guard: Some(Guard(self.clone())),
        }
    }
    pub(crate) fn stop(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stopped = true;
        self.0.stopped.notify(usize::MAX);
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            == 0
    }
    pub(crate) fn wait(&self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.active != 0 {
            state = self
                .0
                .drained
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
    }
}
struct Guard(TaskScope);
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self.0.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active -= 1;
        if state.active == 0 {
            self.0.0.drained.notify_all();
        }
    }
}
pub(crate) struct ScopedFuture {
    future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    listener: Pin<Box<EventListener>>,
    guard: Option<Guard>,
}
impl ScopedFuture {
    fn finish(&mut self) {
        // Release all task-owned protocol buffers and handles before reporting drained.
        self.future.take();
        self.guard.take();
    }
}
impl Future for ScopedFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let stopping = self.listener.as_mut().poll(cx).is_ready();
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(());
        };
        // Let already-resolved replies propagate once before canceling pending
        // work. In particular, Connection.CloseOk must complete its caller.
        if future.as_mut().poll(cx).is_ready() || stopping {
            self.finish();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Drop for ScopedFuture {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct DropProbe(Arc<AtomicBool>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn stop_wakes_sleeping_task_and_drops_resources_before_drained() {
        let scope = TaskScope::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = DropProbe(dropped.clone());
        let task = tokio::spawn(scope.wrap(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        }));
        tokio::task::yield_now().await;
        scope.stop();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(scope.is_finished());
        assert!(dropped.load(Ordering::SeqCst));
        scope.wait();
    }
    #[test]
    fn stop_before_first_poll_and_drop_without_poll_release_resources() {
        let scope = TaskScope::default();
        let task = scope.wrap(std::future::pending());
        assert!(!scope.is_finished());
        drop(task);
        assert!(scope.is_finished());
        scope.stop();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = DropProbe(dropped.clone());
        let _task = scope.wrap(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        });
        assert!(scope.is_finished());
        assert!(dropped.load(Ordering::SeqCst));
    }
}
