use crate::{
    Connection, Error, Promise, PromiseResolver, Result, auth::AuthProvider, promise::Cancelable,
};
use std::{
    fmt,
    sync::{Arc, Mutex, Weak},
};

pub(crate) enum ConnectionStep {
    ProtocolHeader(HandshakeResolver, Connection),
    StartOk(HandshakeResolver, Connection, Arc<dyn AuthProvider>),
    SecureOk(HandshakeResolver, Connection, Arc<dyn AuthProvider>),
}

impl ConnectionStep {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            ConnectionStep::ProtocolHeader(..) => "ProtocolHeader",
            ConnectionStep::StartOk(..) => "StartOk",
            ConnectionStep::SecureOk(..) => "SecureOk",
        }
    }

    pub(crate) fn into_connection_resolver(self) -> (HandshakeResolver, Option<Connection>) {
        match self {
            ConnectionStep::ProtocolHeader(resolver, connection, ..) => {
                (resolver, Some(connection))
            }
            ConnectionStep::StartOk(resolver, connection, ..) => (resolver, Some(connection)),
            ConnectionStep::SecureOk(resolver, connection, ..) => (resolver, Some(connection)),
        }
    }
}

impl fmt::Debug for ConnectionStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionStep::{}", self.name())
    }
}

// Every handshake completion, including authentication task cancellation, goes
// through this owner. Peer Close and a local protocol-violation close reserve
// their cause until the required close write ends.
#[derive(Clone, Debug)]
pub(crate) struct HandshakeResolver(Arc<Mutex<HandshakeCompletion>>);
#[derive(Default, Debug)]
pub(crate) struct WeakHandshakeResolver(Weak<Mutex<HandshakeCompletion>>);
#[derive(Debug)]
struct HandshakeCompletion {
    resolver: Option<PromiseResolver<Connection>>,
    closing: Option<Error>,
}
impl HandshakeResolver {
    pub(crate) fn new() -> (Promise<Connection>, Self) {
        let (promise, resolver) = Promise::new("ProtocolHeader");
        (
            promise,
            Self(Arc::new(Mutex::new(HandshakeCompletion {
                resolver: Some(resolver),
                closing: None,
            }))),
        )
    }
    pub(crate) fn downgrade(&self) -> WeakHandshakeResolver {
        WeakHandshakeResolver(Arc::downgrade(&self.0))
    }
    pub(crate) fn resolve(&self, connection: Connection) {
        let _ = self.complete(Ok(connection));
    }
    pub(crate) fn reject(&self, error: Error) {
        let _ = self.complete(Err(error));
    }
    pub(crate) fn finish_auth(&self, result: Result<()>) -> Result<()> {
        if let Err(error) = &result
            && !self.complete(Err(error.clone()))
        {
            // The close owner also owns retirement; an auth task must not
            // send a competing SetConnectionError while its write is pending.
            return Ok(());
        }
        result
    }
    fn complete(&self, result: Result<Connection>) -> bool {
        let resolver = {
            let mut inner = self.0.lock().unwrap_or_else(|e| e.into_inner());
            if inner.closing.is_some() {
                return false;
            }
            inner.resolver.take()
        };
        if let Some(resolver) = resolver {
            resolver.complete(result);
        }
        true
    }
    pub(crate) fn finish_close(&self) {
        let completion = {
            let mut inner = self.0.lock().unwrap_or_else(|e| e.into_inner());
            inner.resolver.take().zip(inner.closing.clone())
        };
        if let Some((resolver, error)) = completion {
            resolver.reject(error);
        }
    }
}
impl Cancelable for HandshakeResolver {
    fn cancel(&self, error: Error) {
        self.reject(error);
    }
}
impl WeakHandshakeResolver {
    pub(crate) fn defer_close(&self, error: Error) -> Option<HandshakeResolver> {
        let owner = HandshakeResolver(self.0.upgrade()?);
        {
            let mut inner = owner.0.lock().unwrap_or_else(|e| e.into_inner());
            inner.resolver.as_ref()?;
            inner.closing.get_or_insert(error);
        }
        Some(owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    #[test]
    fn peer_close_owns_handshake_completion_until_its_write_finishes() {
        let (promise, resolver) = HandshakeResolver::new();
        let cause = Arc::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let context = Box::new(std::io::Error::from(std::io::ErrorKind::Interrupted));
        let context_address = context.as_ref() as *const std::io::Error;
        let envelope = Arc::new(crate::error::ReceivedClose::new(
            crate::RemoteClose::Connection(crate::protocol::connection::Close {
                reply_code: 599,
                reply_text: "peer close".into(),
                class_id: 0,
                method_id: 0,
            }),
            Some(context),
        ));
        let error = Error::from(ErrorKind::IOError(cause.clone()))
            .with_remote_close(Some(envelope.clone()));
        let owner = resolver.downgrade().defer_close(error.clone()).unwrap();
        let late: Error = ErrorKind::AuthProviderError("late failure".into()).into();
        resolver.reject(late.clone());
        assert!(resolver.finish_auth(Err(late.clone())).is_ok());
        resolver.cancel(late.clone());
        drop(resolver);
        assert!(promise.try_wait().is_none());
        owner.finish_close();
        let received = promise.try_wait().unwrap().unwrap_err();
        assert!(
            matches!(received.kind(), ErrorKind::IOError(received) if Arc::ptr_eq(&cause, received))
        );
        assert!(Arc::ptr_eq(
            &received.remote_close_shared().unwrap(),
            &envelope
        ));
        assert!(std::ptr::eq(
            received
                .remote_close_context()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap(),
            context_address,
        ));
        assert!(owner.finish_auth(Err(late)).is_ok());
        owner.finish_close();
        assert!(promise.try_wait().is_none());
    }
}
