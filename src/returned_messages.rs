use crate::{
    BasicProperties, Promise, message::BasicReturnMessage, publisher_confirm::Confirmation,
    types::PayloadSize,
};
use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};
use tracing::{trace, warn};

#[derive(Clone, Default)]
pub(crate) struct ReturnedMessages(Arc<Mutex<Inner>>);

impl ReturnedMessages {
    pub(crate) fn start_new_delivery(&self, message: BasicReturnMessage) {
        self.lock_inner().current_message = Some(message);
    }

    pub(crate) fn handle_content_header_frame(
        &self,
        size: PayloadSize,
        properties: BasicProperties,
        reservation: Option<crate::limits::Reservation>,
        confirm_mode: bool,
    ) {
        self.lock_inner()
            .handle_content_header_frame(size, properties, reservation, confirm_mode);
    }

    pub(crate) fn handle_body_frame(
        &self,
        remaining_size: PayloadSize,
        payload: Vec<u8>,
        confirm_mode: bool,
    ) {
        self.lock_inner()
            .handle_body_frame(remaining_size, payload, confirm_mode);
    }

    pub(crate) fn drain(&self) -> Vec<BasicReturnMessage> {
        self.lock_inner().drain()
    }

    pub(crate) fn register_dropped_confirm(&self, promise: Promise<Confirmation>) {
        self.lock_inner().register_dropped_confirm(promise);
    }

    pub(crate) fn get_waiting_message(&self) -> Option<BasicReturnMessage> {
        self.lock_inner().waiting_messages.pop_front()
    }

    pub(crate) fn take_dropped_confirms(&self) -> Vec<Promise<Confirmation>> {
        std::mem::take(&mut self.lock_inner().dropped_confirms)
    }

    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl fmt::Debug for ReturnedMessages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ReturnedMessages");
        if let Ok(inner) = self.0.try_lock() {
            debug
                .field("waiting_messages", &inner.waiting_messages)
                .field("non_confirm_messages", &inner.non_confirm_messages);
        }
        debug.finish()
    }
}

#[derive(Default)]
struct Inner {
    current_message: Option<BasicReturnMessage>,
    non_confirm_messages: Vec<BasicReturnMessage>,
    waiting_messages: VecDeque<BasicReturnMessage>,
    dropped_confirms: Vec<Promise<Confirmation>>,
}

impl Inner {
    fn handle_content_header_frame(
        &mut self,
        size: PayloadSize,
        properties: BasicProperties,
        reservation: Option<crate::limits::Reservation>,
        confirm_mode: bool,
    ) {
        if let Some(message) = self.current_message.as_mut() {
            message.properties = properties;
            message.reservation = reservation;
        }
        if size == 0 {
            self.new_delivery_complete(confirm_mode);
        }
    }

    fn handle_body_frame(
        &mut self,
        remaining_size: PayloadSize,
        payload: Vec<u8>,
        confirm_mode: bool,
    ) {
        if let Some(message) = self.current_message.as_mut() {
            message.receive_content(payload);
        }
        if remaining_size == 0 {
            self.new_delivery_complete(confirm_mode);
        }
    }

    fn new_delivery_complete(&mut self, confirm_mode: bool) {
        if let Some(message) = self.current_message.take() {
            warn!("Server returned a message");
            if confirm_mode {
                self.waiting_messages.push_back(message);
            } else {
                self.non_confirm_messages.push(message);
            }
        }
    }

    fn process_dropped_confirm(
        &mut self,
        promise: Promise<Confirmation>,
        messages: &mut Vec<BasicReturnMessage>,
    ) {
        if let Some(confirmation) = promise.try_wait() {
            if let Ok(Confirmation::Nack(Some(message))) | Ok(Confirmation::Ack(Some(message))) =
                confirmation
            {
                trace!("PublisherConfirm was carrying a message, storing it");
                messages.push(message);
            } else {
                trace!("PublisherConfirm was ready but didn't carry a message, discarding");
            }
        } else {
            trace!("PublisherConfirm wasn't ready yet, storing it for further use");
            self.dropped_confirms.push(promise);
        }
    }

    fn register_dropped_confirm(&mut self, promise: Promise<Confirmation>) {
        trace!("Registering new dropped PublisherConfirm");
        self.dropped_confirms.push(promise);
    }

    fn drain(&mut self) -> Vec<BasicReturnMessage> {
        let mut messages = std::mem::take(&mut self.non_confirm_messages);
        let before = self.dropped_confirms.len();
        if before != 0 {
            for promise in std::mem::take(&mut self.dropped_confirms) {
                self.process_dropped_confirm(promise, &mut messages)
            }
            trace!(
                %before,
                after=%self.dropped_confirms.len(),
                "PublisherConfirms processed"
            );
        }
        messages
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use crate::{
        ErrorKind, acknowledgement::Acknowledgements, publisher_confirm::PublisherConfirm,
    };

    #[test]
    fn dropped_confirms_retain_ack_nack_and_error_for_their_owner() {
        for ready_before_drop in [false, true] {
            for outcome in 0..3 {
                let returned = ReturnedMessages::default();
                let acknowledgements = Acknowledgements::new(1, returned.clone());
                let confirm = acknowledgements.register_pending();
                let complete = || match outcome {
                    0 => acknowledgements.ack(1).unwrap(),
                    1 => acknowledgements.nack(1).unwrap(),
                    _ => acknowledgements.on_channel_error(ErrorKind::MissingHeartbeatError.into()),
                };
                if ready_before_drop {
                    complete();
                    drop(confirm);
                } else {
                    drop(confirm);
                    complete();
                }
                let mut inner = returned.lock_inner();
                assert_eq!(
                    inner.dropped_confirms.len(),
                    1,
                    "outcome {outcome}, ready before drop {ready_before_drop}"
                );
                let result = inner.dropped_confirms.pop().unwrap().try_wait().unwrap();
                match outcome {
                    0 => assert!(matches!(result, Ok(Confirmation::Ack(None)))),
                    1 => assert!(matches!(result, Ok(Confirmation::Nack(None)))),
                    _ => assert!(
                        matches!(result, Err(error) if matches!(error.kind(), ErrorKind::MissingHeartbeatError))
                    ),
                }
            }
        }
        // A confirm which never needed a broker result still retains that fact.
        let returned = ReturnedMessages::default();
        drop(PublisherConfirm::not_requested(returned.clone()));
        assert!(matches!(
            returned
                .lock_inner()
                .dropped_confirms
                .pop()
                .unwrap()
                .try_wait(),
            Some(Ok(Confirmation::NotRequested))
        ));
    }
}
