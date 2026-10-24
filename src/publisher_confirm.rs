use crate::{
    ErrorKind, Promise, Result, message::BasicReturnMessage, returned_messages::ReturnedMessages,
};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::trace;

/// A future that resolves to the broker's acknowledgement of a published message.
///
/// Returned by [`Channel::basic_publish`]. If publisher confirms are not
/// enabled (via [`Channel::confirm_select`]) the future resolves immediately
/// with [`Confirmation::NotRequested`].
///
/// Dropping an unresolved `PublisherConfirm` without awaiting it registers it
/// with [`Channel::wait_for_confirms`] so that confirms are not silently lost.
///
/// [`Channel::basic_publish`]: crate::Channel::basic_publish
/// [`Channel::confirm_select`]: crate::Channel::confirm_select
/// [`Channel::wait_for_confirms`]: crate::Channel::wait_for_confirms
pub struct PublisherConfirm {
    inner: Option<Promise<Confirmation>>,
    returned_messages: ReturnedMessages,
}

/// The broker's response to a published message.
#[derive(Debug, PartialEq)]
pub enum Confirmation {
    /// The broker acknowledged the message. The inner value carries the
    /// returned message if the publish was mandatory/immediate and unroutable.
    Ack(Option<BasicReturnMessage>),
    /// The broker negatively acknowledged the message (could not route or
    /// store it). The inner value carries the returned message if available.
    Nack(Option<BasicReturnMessage>),
    /// Publisher confirms were not enabled; the broker did not confirm.
    NotRequested,
}

impl Confirmation {
    /// Extract the returned message, if any.
    ///
    /// Returns `Some` only when the message was unroutable and returned by the
    /// broker (mandatory or immediate publish with no matching route).
    #[must_use]
    pub fn take_message(self) -> Option<BasicReturnMessage> {
        if let Confirmation::Ack(msg) | Confirmation::Nack(msg) = self {
            msg
        } else {
            None
        }
    }

    /// Returns `true` if the broker acknowledged the message.
    #[must_use]
    pub fn is_ack(&self) -> bool {
        matches!(self, Confirmation::Ack(_))
    }

    /// Returns `true` if the broker negatively acknowledged the message.
    #[must_use]
    pub fn is_nack(&self) -> bool {
        matches!(self, Confirmation::Nack(_))
    }
}

impl PublisherConfirm {
    /// Transfer this publication's native receiver to its outcome owner.
    /// Unlike the legacy returned-message drain, this retains Ack, Nack and
    /// errors, including an attached returned message and its reservation.
    pub fn into_completion(mut self) -> crate::Completion<Confirmation> {
        self.inner.take().unwrap_or_else(|| {
            Promise::new_with_data(
                "publisher-confirms.completed",
                Err(ErrorKind::FutureCompleted.into()),
            )
        })
    }

    pub(crate) fn new(inner: Promise<Confirmation>, returned_messages: ReturnedMessages) -> Self {
        Self {
            inner: Some(inner),
            returned_messages,
        }
    }

    pub(crate) fn not_requested(returned_messages: ReturnedMessages) -> Self {
        Self {
            inner: Some(Promise::new_with_data(
                "publisher-confirms.not-requested",
                Ok(Confirmation::NotRequested),
            )),
            returned_messages,
        }
    }
}

impl fmt::Debug for PublisherConfirm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublisherConfirm").finish()
    }
}

impl Future for PublisherConfirm {
    type Output = Result<Confirmation>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(ErrorKind::FutureCompleted.into()));
        };
        let res = Pin::new(inner).poll(cx);
        if res.is_ready() {
            this.inner.take();
        }
        res
    }
}

impl Drop for PublisherConfirm {
    fn drop(&mut self) {
        if let Some(promise) = self.inner.take() {
            trace!("PublisherConfirm dropped without use, registering it for wait_for_confirms");
            self.returned_messages.register_dropped_confirm(promise);
        }
    }
}
