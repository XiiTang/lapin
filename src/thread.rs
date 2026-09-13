use crate::Result;
use std::{
    panic,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};
use tracing::error;

pub(crate) type JoinHandle = thread::JoinHandle<Result<()>>;
type Inner = Option<JoinHandle>;

#[derive(Clone)]
pub(crate) struct ThreadHandle(Arc<Mutex<Inner>>);

impl Default for ThreadHandle {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}

impl ThreadHandle {
    pub(crate) fn register(&self, handle: JoinHandle) {
        *self.lock_inner() = Some(handle);
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.lock_inner()
            .as_ref()
            .is_none_or(|thread| thread.is_finished())
    }

    fn take(&self) -> Option<JoinHandle> {
        self.lock_inner().take()
    }

    pub(crate) fn wait(&self, context: &'static str) -> Result<()> {
        if let Some(handle) = self.take()
            && handle.thread().id() != thread::current().id()
        {
            match handle.join() {
                Ok(res) => return res,
                Err(e) => {
                    if let Some(msg) = e.downcast_ref::<&str>() {
                        error!(%context, panic = msg, "Thread panicked");
                    } else if let Some(msg) = e.downcast_ref::<String>() {
                        error!(%context, panic = msg.as_str(), "Thread panicked");
                    } else {
                        error!(%context, "Thread panicked with unknown payload");
                    }
                    panic::resume_unwind(e);
                }
            }
        }
        Ok(())
    }

    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
