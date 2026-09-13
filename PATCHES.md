# Runtime-controlled transport and bounded delivery

Upstream: https://github.com/amqp-rs/lapin, tag v4.11.0,
base `0dc84fc1f6736817b75f7699e18ea2b08d3ed45f`.
Branch: `codex/runtime-driver`. Original upstream history is retained.

The existing connector, parser, channel state, content assembly, confirms,
transactions and recovery engine remain the only protocol implementation.

- Supplied streams and caller-controlled reconnect connectors return a physical
  driver owner before the handshake is polled. Explicit asynchronous auth is
  required; token refresh and implicit connection/channel/consumer cleanup are
  disabled for these entry points. Normal upstream entry points keep their policy.
- Stop wakes physical I/O, cancels scoped RPC/auth/heartbeat work, and join waits
  for the I/O thread and owned resources. Wakeup notifications coalesce in one slot.
- ReceiveLimits checks advertised frame/body sizes before body allocation and
  reserves aggregate message bytes/count until Delivery/Get/Return values drop.
  Event and internal RPC queues are bounded and fail closed on overflow.
- Topology retention has an aggregate metadata budget; duplicate bindings are
  deduplicated. Consumer payload queues retain the native delivery reservations.
- Reconnect, topology restoration and consumer restoration are separate settings.
  The caller bounds connector attempts. Pending publications and transactions
  are never replayed. Channel generation invalidation is synchronized with frame
  admission; stale acknowledgement/publication handles cannot enqueue work.
- The existing amq-protocol parser fork bounds recursive field values to 16
  containers and erases LongString allocations on drop. Transport buffers are
  erased on consume/reset/drop. Logs exclude payloads/auth frames; error Display
  excludes untrusted auth-provider and peer text. Explicit error kind access
  preserves protocol response facts for the embedding application's result path.

Validated on macOS, no default features, `tokio`:

```
cargo test --no-default-features --features tokio --lib \
  --test supplied_driver --test connection_retry
cargo clippy --no-default-features --features tokio --lib \
  --test supplied_driver --test connection_retry -- -D warnings
```

26 tests passed (15 unit, 8 supplied-driver, 3 retry/half-close); Clippy passed.
The embedding IMAPipe tests additionally exercise independent wire peers and an
isolated RabbitMQ 4.3.5 process. Their exact matrix belongs to IMAPipe's design
and acceptance documents; these fork tests do not claim other-platform acceptance.
