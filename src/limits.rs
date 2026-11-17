//! Allocation limits shared by every channel of a supplied connection.
use std::sync::{Arc, Mutex};

use crate::socket_state::SocketStateHandle;
/// A connection-wide bound, checked before content assembly.
#[derive(Clone, Copy, Debug)]
pub struct ReceiveLimits {
    /// Maximum bytes in one message body.
    pub message_bytes: usize,
    /// Maximum total body bytes retained by all deliveries.
    pub retained_bytes: usize,
    /// Maximum total number of retained deliveries, including empty messages.
    pub retained_messages: usize,
}
/// Which local receive or queue bound was exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceLimit {
    /// Bytes in one received message body.
    MessageBytes,
    /// Total body bytes retained by deliveries.
    RetainedBytes,
    /// Number of retained deliveries, including empty messages.
    RetainedMessages,
    /// Number of pending connection events.
    EventQueueMessages,
    /// Number of pending internal RPC commands.
    InternalRpcQueueMessages,
}
impl ResourceLimit {
    pub(crate) fn exceeded(self, maximum: usize, observed: Option<u64>) -> crate::Error {
        crate::ErrorKind::ResourceLimitExceeded {
            limit: self,
            maximum: maximum as u64,
            observed,
        }
        .into()
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Budget(Arc<Inner>);
struct Inner {
    limits: ReceiveLimits,
    used: Mutex<Used>,
    /// The I/O loop, woken when a release makes room it waits for.
    waker: Mutex<Option<SocketStateHandle>>,
}
#[derive(Default)]
struct Used {
    bytes: usize,
    messages: usize,
    /// The I/O loop holds a content header until room returns.
    waiting: bool,
}
impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("limits", &self.limits)
            .finish()
    }
}
impl Budget {
    pub(crate) fn new(limits: ReceiveLimits) -> Self {
        Self(Arc::new(Inner {
            limits,
            used: Mutex::default(),
            waker: Mutex::default(),
        }))
    }
    /// Wakes the I/O loop that reads this connection when room returns.
    pub(crate) fn bind(&self, waker: SocketStateHandle) {
        *self.0.waker.lock().unwrap_or_else(|e| e.into_inner()) = Some(waker);
    }
    /// Whether the I/O loop holds a header for room.
    #[cfg(test)]
    pub(crate) fn waiting(&self) -> bool {
        self.0
            .used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .waiting
    }
    /// Whether a body of `bytes` has room now. A body that can never fit
    /// has room: [`Self::reserve`] refuses it. One that has none now waits
    /// for a release, which wakes the I/O loop; it always comes, because
    /// `retained_bytes` is at least `message_bytes`.
    pub(crate) fn has_room(&self, bytes: u64) -> bool {
        let limits = self.0.limits;
        if bytes > limits.message_bytes as u64 {
            return true;
        }
        let mut used = self.0.used.lock().unwrap_or_else(|e| e.into_inner());
        let room = used
            .bytes
            .checked_add(bytes as usize)
            .is_some_and(|total| total <= limits.retained_bytes)
            && used.messages < limits.retained_messages;
        used.waiting = !room;
        room
    }
    pub(crate) fn reserve(&self, bytes: u64) -> crate::Result<Reservation> {
        let limits = self.0.limits;
        if bytes > limits.message_bytes as u64 {
            return Err(ResourceLimit::MessageBytes.exceeded(limits.message_bytes, Some(bytes)));
        }
        let bytes = bytes as usize;
        let mut used = self.0.used.lock().unwrap_or_else(|e| e.into_inner());
        let total = used.bytes.checked_add(bytes);
        if total.is_none_or(|total| total > limits.retained_bytes) {
            return Err(ResourceLimit::RetainedBytes
                .exceeded(limits.retained_bytes, total.map(|n| n as u64)));
        }
        if used.messages >= limits.retained_messages {
            return Err(ResourceLimit::RetainedMessages.exceeded(
                limits.retained_messages,
                (used.messages as u64).checked_add(1),
            ));
        }
        used.bytes = total.unwrap();
        used.messages += 1;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Budget,
    bytes: usize,
}
// Resource ownership does not change a message's protocol equality.
impl PartialEq for Reservation {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let waiting = {
            let mut used = self.budget.0.used.lock().unwrap_or_else(|e| e.into_inner());
            used.bytes -= self.bytes;
            used.messages -= 1;
            std::mem::take(&mut used.waiting)
        };
        if waiting
            && let Some(waker) = &*self
                .budget
                .0
                .waker
                .lock()
                .unwrap_or_else(|e| e.into_inner())
        {
            waker.wake();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn assert_limit(
        error: crate::Error,
        limit: ResourceLimit,
        maximum: u64,
        observed: Option<u64>,
    ) {
        assert!(
            matches!(error.kind(), crate::ErrorKind::ResourceLimitExceeded {
            limit: actual_limit, maximum: actual_maximum, observed: actual_observed,
        } if (*actual_limit, *actual_maximum, *actual_observed) == (limit, maximum, observed)),
            "{error}"
        );
        assert!(!error.can_be_recovered());
    }
    #[test]
    fn retains_capacity_until_last_message_is_dropped() {
        let b = Budget::new(ReceiveLimits {
            message_bytes: 8,
            retained_bytes: 10,
            retained_messages: 2,
        });
        assert_limit(
            b.reserve(9).unwrap_err(),
            ResourceLimit::MessageBytes,
            8,
            Some(9),
        );
        assert_limit(
            b.reserve(u64::MAX).unwrap_err(),
            ResourceLimit::MessageBytes,
            8,
            Some(u64::MAX),
        );
        let a = b.reserve(8).unwrap();
        assert_limit(
            b.reserve(3).unwrap_err(),
            ResourceLimit::RetainedBytes,
            10,
            Some(11),
        );
        let z = b.reserve(0).unwrap();
        assert_limit(
            b.reserve(0).unwrap_err(),
            ResourceLimit::RetainedMessages,
            2,
            Some(3),
        );
        drop(a);
        drop(z);
        assert!(b.reserve(8).is_ok());
    }
    #[test]
    fn a_body_without_room_waits_and_the_release_wakes_the_reader() {
        let b = Budget::new(ReceiveLimits {
            message_bytes: 8,
            retained_bytes: 10,
            retained_messages: 2,
        });
        let state = crate::socket_state::SocketState::default();
        b.bind(state.handle());
        // A body that can never fit is not waited for: reserve refuses it.
        assert!(b.has_room(9));
        let a = b.reserve(8).unwrap();
        // A release nothing waits for wakes no one.
        drop(b.reserve(0).unwrap());
        assert!(!state.woken());
        assert!(!b.has_room(3));
        let z = b.reserve(0).unwrap();
        assert!(!b.has_room(0));
        drop(z);
        assert!(state.woken());
        assert!(b.has_room(0));
        assert!(!b.has_room(3));
        drop(a);
        assert!(b.has_room(8));
    }
}
