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
        let bytes = usize::try_from(bytes).map_err(|_| exceeded())?;
        let mut used = self.0.used.lock().unwrap_or_else(|e| e.into_inner());
        let total = used.0.checked_add(bytes).ok_or_else(exceeded)?;
        if bytes > self.0.limits.message_bytes
            || total > self.0.limits.retained_bytes
            || used.1 >= self.0.limits.retained_messages
        {
            return Err(exceeded());
        }
        used.0 = total;
        used.1 += 1;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}
fn exceeded() -> crate::Error {
    crate::ErrorKind::ResourceLimitExceeded.into()
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
impl Reservation {
    pub(crate) fn resize(&mut self, bytes: usize) -> crate::Result<()> {
        let mut used = self.budget.0.used.lock().unwrap_or_else(|e| e.into_inner());
        let total = (used.0 - self.bytes)
            .checked_add(bytes)
            .ok_or_else(exceeded)?;
        if total > self.budget.0.limits.retained_bytes {
            return Err(exceeded());
        }
        used.0 = total;
        self.bytes = bytes;
        Ok(())
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
    #[test]
    fn retains_capacity_until_last_message_is_dropped() {
        let b = Budget::new(ReceiveLimits {
            message_bytes: 8,
            retained_bytes: 10,
            retained_messages: 2,
        });
        assert!(b.reserve(9).is_err());
        let a = b.reserve(8).unwrap();
        assert!(b.reserve(3).is_err());
        let z = b.reserve(0).unwrap();
        assert!(b.reserve(0).is_err());
        drop(a);
        drop(z);
        assert!(b.reserve(8).is_ok());
    }
}
