use crate::{
    exchange::ExchangeKind,
    options::{ExchangeDeclareOptions, QueueDeclareOptions},
    shared::SharedMutex,
    topology::{ExchangeDefinition, QueueDefinition},
    types::{FieldTable, ShortString},
};
use std::{collections::HashMap, sync::MutexGuard};

#[derive(Clone, Default)]
pub(crate) struct Registry(SharedMutex<Inner>);

impl Registry {
    pub(crate) fn new(budget: Option<crate::limits::Budget>) -> Self {
        let registry = Self::default();
        registry.lock_inner().budget = budget;
        registry
    }
    fn check_limit(&self) -> crate::Result<()> {
        let mut inner = self.lock_inner();
        let mut bytes = 0usize;
        for ex in inner.exchanges.values() {
            bytes +=
                512 + ex.name.as_str().len() * 2 + ex.arguments.as_ref().map_or(0, table_bytes);
            for b in &ex.bindings {
                bytes += 256
                    + b.source.as_str().len() * 2
                    + b.routing_key.as_str().len() * 2
                    + table_bytes(&b.arguments);
            }
        }
        for q in inner.queues.values() {
            bytes += 512 + q.name.as_str().len() * 2 + q.arguments.as_ref().map_or(0, table_bytes);
            for b in &q.bindings {
                bytes += 256
                    + b.source.as_str().len() * 2
                    + b.routing_key.as_str().len() * 2
                    + table_bytes(&b.arguments);
            }
        }
        if let Some(reservation) = inner.reservation.as_mut() {
            reservation.resize(bytes)?;
        } else if let Some(budget) = &inner.budget {
            inner.reservation = Some(budget.reserve(bytes as u64)?);
        }
        Ok(())
    }

    pub(crate) fn exchanges_topology(&self) -> Vec<ExchangeDefinition> {
        self.lock_inner().exchanges.values().cloned().collect()
    }

    pub(crate) fn queues_topology(&self) -> Vec<QueueDefinition> {
        self.lock_inner().queues.values().cloned().collect()
    }

    pub(crate) fn register_exchange(
        &self,
        name: ShortString,
        kind: ExchangeKind,
        options: ExchangeDeclareOptions,
        arguments: FieldTable,
    ) -> crate::Result<()> {
        let mut inner = self.lock_inner();
        if let Some(exchange) = inner.exchanges.get_mut(&name) {
            exchange.set_declared(kind, options, arguments);
        } else {
            inner.exchanges.insert(
                name.clone(),
                ExchangeDefinition::declared(name, kind, options, arguments),
            );
        }
        drop(inner);
        self.check_limit()
    }

    pub(crate) fn deregister_exchange(&self, name: &str) {
        self.lock_inner().exchanges.remove(name);
        let _ = self.check_limit();
    }

    pub(crate) fn register_exchange_binding(
        &self,
        destination: ShortString,
        source: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> crate::Result<()> {
        self.lock_inner()
            .exchanges
            .entry(destination.clone())
            .or_insert_with(|| ExchangeDefinition::undeclared(destination))
            .register_binding(source, routing_key, arguments);
        self.check_limit()
    }

    pub(crate) fn deregister_exchange_binding(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) {
        if let Some(destination) = self.lock_inner().exchanges.get_mut(destination) {
            destination.deregister_binding(source, routing_key, arguments);
        }
        let _ = self.check_limit();
    }

    pub(crate) fn register_queue(
        &self,
        name: ShortString,
        options: QueueDeclareOptions,
        arguments: FieldTable,
    ) -> crate::Result<()> {
        let mut inner = self.lock_inner();
        if let Some(queue) = inner.queues.get_mut(&name) {
            queue.set_declared(options, arguments);
        } else {
            inner.queues.insert(
                name.clone(),
                QueueDefinition::declared(name, options, arguments),
            );
        }
        drop(inner);
        self.check_limit()
    }

    pub(crate) fn deregister_queue(&self, name: &str) {
        self.lock_inner().queues.remove(name);
        let _ = self.check_limit();
    }

    pub(crate) fn register_queue_binding(
        &self,
        destination: ShortString,
        source: ShortString,
        routing_key: ShortString,
        arguments: FieldTable,
    ) -> crate::Result<()> {
        self.lock_inner()
            .queues
            .entry(destination.clone())
            .or_insert_with(|| QueueDefinition::undeclared(destination))
            .register_binding(source, routing_key, arguments);
        self.check_limit()
    }

    pub(crate) fn deregister_queue_binding(
        &self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) {
        if let Some(destination) = self.lock_inner().queues.get_mut(destination) {
            destination.deregister_binding(source, routing_key, arguments);
        }
        let _ = self.check_limit();
    }

    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock()
    }
}

#[derive(Default)]
struct Inner {
    budget: Option<crate::limits::Budget>,
    reservation: Option<crate::limits::Reservation>,
    exchanges: HashMap<ShortString, ExchangeDefinition>,
    queues: HashMap<ShortString, QueueDefinition>,
}

fn table_bytes(table: &FieldTable) -> usize {
    table
        .into_iter()
        .map(|(k, v)| 128 + k.as_str().len() * 2 + value_bytes(v))
        .sum()
}
fn value_bytes(v: &crate::types::AMQPValue) -> usize {
    use crate::types::AMQPValue::*;
    128 + match v {
        LongString(v) => v.as_bytes().len() * 2,
        ShortString(v) => v.as_str().len() * 2,
        ByteArray(v) => v.as_slice().len() * 2,
        FieldTable(v) => table_bytes(v),
        FieldArray(v) => v.as_slice().iter().map(value_bytes).sum(),
        _ => 0,
    }
}
