use crate::{
    exchange::ExchangeKind,
    options::{ExchangeDeclareOptions, QueueDeclareOptions},
    shared::SharedMutex,
    topology::{ExchangeDefinition, QueueDefinition},
    types::{FieldTable, ShortString},
};
use std::{collections::HashMap, sync::MutexGuard};

#[derive(Clone)]
pub(crate) struct Registry {
    collect_topology: bool,
    inner: SharedMutex<Inner>,
}

impl Registry {
    pub(crate) fn new(collect_topology: bool, budget: Option<crate::limits::Budget>) -> Self {
        Self {
            collect_topology,
            inner: SharedMutex::new(Inner {
                budget,
                ..Inner::default()
            }),
        }
    }
    fn check_limit(&self) -> crate::Result<()> {
        if !self.collect_topology {
            return Ok(());
        }
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
        if !self.collect_topology {
            return Vec::new();
        }
        self.lock_inner().exchanges.values().cloned().collect()
    }

    pub(crate) fn queues_topology(&self) -> Vec<QueueDefinition> {
        if !self.collect_topology {
            return Vec::new();
        }
        self.lock_inner().queues.values().cloned().collect()
    }

    pub(crate) fn register_exchange(
        &self,
        name: ShortString,
        kind: ExchangeKind,
        options: ExchangeDeclareOptions,
        arguments: FieldTable,
    ) -> crate::Result<()> {
        if !self.collect_topology {
            return Ok(());
        }
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
        if !self.collect_topology {
            return Ok(());
        }
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
        server_generated: bool,
    ) -> crate::Result<()> {
        if !self.collect_topology {
            return Ok(());
        }
        let mut inner = self.lock_inner();
        if let Some(queue) = inner.queues.get_mut(&name) {
            queue.set_declared(options, arguments);
            queue.server_generated |= server_generated;
        } else {
            inner.queues.insert(
                name.clone(),
                QueueDefinition::declared(name, options, server_generated, arguments),
            );
        }
        drop(inner);
        self.check_limit()
    }

    pub(crate) fn remap_queue(
        &self,
        previous: &ShortString,
        current: &ShortString,
    ) -> crate::Result<()> {
        if !self.collect_topology || previous == current {
            return Ok(());
        }
        let mut inner = self.lock_inner();
        if let Some(mut queue) = inner.queues.remove(previous) {
            queue.name = current.clone();
            if let Some(existing) = inner.queues.get_mut(current) {
                for binding in queue.bindings {
                    existing.register_binding(
                        binding.source,
                        binding.routing_key,
                        binding.arguments,
                    );
                }
            } else {
                inner.queues.insert(current.clone(), queue);
            }
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
        if !self.collect_topology {
            return Ok(());
        }
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
        self.inner.lock()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_empty(registry: &Registry) {
        // Inspect storage as well as extraction: hiding retained entries is insufficient.
        let inner = registry.lock_inner();
        assert!(inner.exchanges.is_empty());
        assert!(inner.queues.is_empty());
        assert!(inner.reservation.is_none());
        drop(inner);
        assert!(registry.exchanges_topology().is_empty());
        assert!(registry.queues_topology().is_empty());
    }

    #[test]
    fn disabled_collection_retains_no_entries_or_budget() {
        let budget = crate::limits::Budget::new(crate::ReceiveLimits {
            message_bytes: 0,
            retained_bytes: 0,
            retained_messages: 0,
        });
        for budget in [None, Some(budget)] {
            let registry = Registry::new(false, budget);
            let clone = registry.clone();
            for registry in [&registry, &clone] {
                registry
                    .register_exchange(
                        "declared".into(),
                        ExchangeKind::Direct,
                        ExchangeDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .unwrap();
                registry
                    .register_queue(
                        "queue".into(),
                        QueueDeclareOptions::default(),
                        FieldTable::default(),
                        true,
                    )
                    .unwrap();
                // Neither destination was declared through this registry.
                registry
                    .register_exchange_binding(
                        "external-exchange".into(),
                        "external-source".into(),
                        "key".into(),
                        FieldTable::default(),
                    )
                    .unwrap();
                registry
                    .register_queue_binding(
                        "external-queue".into(),
                        "external-source".into(),
                        "key".into(),
                        FieldTable::default(),
                    )
                    .unwrap();
                registry
                    .remap_queue(&"queue".into(), &"renamed".into())
                    .unwrap();
                registry
                    .remap_queue(&"external-queue".into(), &"renamed".into())
                    .unwrap();
                assert_empty(registry);
                registry.deregister_exchange_binding(
                    "external-exchange",
                    "external-source",
                    "key",
                    &FieldTable::default(),
                );
                registry.deregister_queue_binding(
                    "external-queue",
                    "external-source",
                    "key",
                    &FieldTable::default(),
                );
                registry.deregister_exchange("declared");
                registry.deregister_queue("renamed");

                assert_empty(registry);
            }
        }
    }

    #[test]
    fn enabled_collection_keeps_external_bindings_and_remaps_queues() {
        let registry = Registry::new(true, None);
        registry
            .register_queue_binding(
                "external".into(),
                "source".into(),
                "key".into(),
                FieldTable::default(),
            )
            .unwrap();
        registry
            .register_queue(
                "declared".into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
                true,
            )
            .unwrap();
        registry
            .remap_queue(&"external".into(), &"declared".into())
            .unwrap();
        registry
            .remap_queue(&"declared".into(), &"renamed".into())
            .unwrap();
        let queues = registry.queues_topology();
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].name.as_str(), "renamed");
        assert!(queues[0].is_declared);
        assert!(queues[0].server_generated);
        assert_eq!(queues[0].bindings.len(), 1);
        registry
            .register_exchange_binding(
                "external".into(),
                "source".into(),
                "key".into(),
                FieldTable::default(),
            )
            .unwrap();
        let exchanges = registry.exchanges_topology();
        assert_eq!(exchanges.len(), 1);
        assert!(!exchanges[0].is_declared);
        assert_eq!(exchanges[0].bindings.len(), 1);
    }
}
