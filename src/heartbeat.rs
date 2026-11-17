use crate::{ConnectionStatus, ErrorKind, internal_rpc::InternalRPCHandle, killswitch::KillSwitch};
use async_rs::{Runtime, traits::*};
use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tracing::error;

#[derive(Clone)]
pub(crate) struct Heartbeat<RK: RuntimeKit + Clone + Send + 'static> {
    connection_status: ConnectionStatus,
    killswitch: KillSwitch,
    runtime: Runtime<RK>,
    inner: Arc<Mutex<Inner>>,
}

impl<RK: RuntimeKit + Clone + Send + 'static> Heartbeat<RK> {
    pub(crate) fn new(connection_status: ConnectionStatus, runtime: Runtime<RK>) -> Self {
        let killswitch = Default::default();
        let inner = Default::default();
        Self {
            connection_status,
            killswitch,
            runtime,
            inner,
        }
    }

    pub(crate) fn set_timeout(&self, timeout: Duration) {
        self.lock_inner().timeout = Some(timeout);
    }

    pub(crate) fn killswitch(&self) -> KillSwitch {
        self.killswitch.clone()
    }

    pub(crate) fn start(&self, internal_rpc: InternalRPCHandle) {
        let heartbeat = self.clone();
        let poison = self.lock_inner().poison.clone();
        self.runtime
            .spawn(internal_rpc.task_scope.clone().wrap(async move {
                while let Some(dur) = heartbeat.poll_timeout(&internal_rpc, &poison) {
                    heartbeat.runtime.sleep(dur).await;
                }
            }));
    }

    fn poll_timeout(
        &self,
        internal_rpc: &InternalRPCHandle,
        poison: &KillSwitch,
    ) -> Option<Duration> {
        if poison.killed() {
            return None;
        }

        if !self.connection_status.connected() {
            self.cancel();
            return None;
        }

        self.lock_inner()
            .poll_timeout(internal_rpc, &self.killswitch)
    }

    pub(crate) fn update_last_write(&self) {
        self.lock_inner().update_last_write();
    }

    pub(crate) fn update_last_read(&self) {
        self.lock_inner().update_last_read();
    }

    /// The I/O loop stops or resumes reading while it waits for room. The
    /// missing-heartbeat check waits for the peer, so it is skipped while
    /// reading is paused and starts again from the resume.
    pub(crate) fn set_paused(&self, paused: bool) {
        self.lock_inner().set_paused(paused);
    }

    pub(crate) fn cancel(&self) {
        self.lock_inner().cancel();
    }

    pub(crate) fn reset(&self) {
        self.killswitch.reset();
        self.lock_inner().reset();
    }

    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl<RK: RuntimeKit + Clone + Send + 'static> fmt::Debug for Heartbeat<RK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Heartbeat").finish()
    }
}

struct Inner {
    last_read: Instant,
    last_write: Instant,
    timeout: Option<Duration>,
    poison: KillSwitch,
    paused: bool,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            last_read: Instant::now(),
            last_write: Instant::now(),
            timeout: None,
            poison: KillSwitch::default(),
            paused: false,
        }
    }
}

impl Inner {
    fn poll_timeout(
        &mut self,
        internal_rpc: &InternalRPCHandle,
        killswitch: &KillSwitch,
    ) -> Option<Duration> {
        let timeout = self.timeout?;

        // The value stored in timeout is half the configured heartbeat value as the spec recommends to send heartbeats at twice the configured pace.
        // The specs tells us to close the connection after twice the configured interval has passed.
        if !self.paused && Instant::now().duration_since(self.last_read) > timeout.saturating_mul(4)
        {
            error!(
                "We haven't received anything from the server for too long, closing connection."
            );
            self.timeout = None;
            killswitch.kill();
            internal_rpc.set_connection_error(ErrorKind::MissingHeartbeatError.into());
            return None;
        }

        timeout
            .checked_sub(self.last_write.elapsed())
            .map(|timeout| timeout.max(Duration::from_millis(1)))
            .or_else(|| {
                // Update last_write so that if we cannot write to the socket yet, we don't enqueue countless heartbeats
                self.update_last_write();
                internal_rpc.send_heartbeat();
                Some(timeout)
            })
    }

    fn update_last_write(&mut self) {
        self.last_write = Instant::now();
    }

    fn update_last_read(&mut self) {
        self.last_read = Instant::now();
    }

    fn set_paused(&mut self, paused: bool) {
        if self.paused && !paused {
            self.update_last_read();
        }
        self.paused = paused;
    }

    fn cancel(&mut self) {
        self.timeout = None;
        self.poison.kill();
    }

    fn reset(&mut self) {
        self.cancel();
        self.paused = false;
        self.update_last_read();
        self.update_last_write();
        self.poison = KillSwitch::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectionStatus, frames::Frames, internal_rpc::InternalRPC, secret_update::SecretUpdate,
        socket_state::SocketState, uri::AMQPUri,
    };

    #[tokio::test]
    async fn a_paused_read_skips_the_missing_heartbeat_check_and_restarts_it_at_resume() {
        let runtime = Runtime::tokio_current();
        let uri = AMQPUri::default();
        let config = crate::Configuration::new(&uri, crate::ConnectionProperties::default());
        let status = ConnectionStatus::new(&uri);
        let socket = SocketState::default();
        let rpc = InternalRPC::new(
            runtime.clone(),
            Heartbeat::new(status.clone(), runtime.clone()),
            SecretUpdate::new(status.clone(), runtime, config.auth_provider.clone()),
            Frames::default(),
            socket.handle(),
            status,
        );
        let handle = rpc.handle();
        let timeout = Duration::from_millis(500);
        let silent = timeout.saturating_mul(5);
        let killswitch = KillSwitch::default();
        let mut inner = Inner {
            timeout: Some(timeout),
            ..Default::default()
        };
        inner.last_read -= silent;
        inner.set_paused(true);
        assert!(inner.poll_timeout(&handle, &killswitch).is_some());
        assert!(!killswitch.killed());
        // The resume is the last read; silence after it is judged from there.
        inner.set_paused(false);
        assert!(inner.poll_timeout(&handle, &killswitch).is_some());
        assert!(!killswitch.killed());
        inner.last_read -= silent;
        assert!(inner.poll_timeout(&handle, &killswitch).is_none());
        assert!(killswitch.killed());
    }
}
