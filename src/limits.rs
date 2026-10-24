//! Allocation limits shared by every channel of a supplied connection.
use std::sync::{Arc, Mutex};
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
#[derive(Debug)]
struct Inner {
    limits: ReceiveLimits,
    used: Mutex<(usize, usize)>,
}
impl Budget {
    pub(crate) fn new(limits: ReceiveLimits) -> Self {
        Self(Arc::new(Inner {
            limits,
            used: Mutex::new((0, 0)),
        }))
    }
    pub(crate) fn reserve(&self, bytes: u64) -> crate::Result<Reservation> {
        let limits = self.0.limits;
        if bytes > limits.message_bytes as u64 {
            return Err(ResourceLimit::MessageBytes.exceeded(limits.message_bytes, Some(bytes)));
        }
        let bytes = bytes as usize;
        let mut used = self.0.used.lock().unwrap_or_else(|e| e.into_inner());
        let total = used.0.checked_add(bytes);
        if total.is_none_or(|total| total > limits.retained_bytes) {
            return Err(ResourceLimit::RetainedBytes
                .exceeded(limits.retained_bytes, total.map(|n| n as u64)));
        }
        if used.1 >= limits.retained_messages {
            return Err(ResourceLimit::RetainedMessages
                .exceeded(limits.retained_messages, (used.1 as u64).checked_add(1)));
        }
        used.0 = total.unwrap();
        used.1 += 1;
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
        let mut used = self.budget.0.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 -= self.bytes;
        used.1 -= 1;
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
}
