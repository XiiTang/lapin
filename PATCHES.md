# Runtime-owned supplied transport checkpoint

Upstream: https://github.com/amqp-rs/lapin, tag v4.11.0,
base commit `0dc84fc1f6736817b75f7699e18ea2b08d3ed45f`.
Branch: `codex/runtime-driver`. Upstream history is retained.

This is an integration checkpoint, not a completed IMAPipe AMQP replacement.

- Generalize the existing connector and I/O loop over caller-supplied async
  streams; no second frame parser, handshake or connection driver is introduced.
- `Connection::from_stream` returns a physical driver owner before the handshake
  future is polled. Require explicit authentication with no automatic refresh,
  reject automatic recovery, force zero reconnect attempts and disable implicit
  Connection.Close on connection drop. The caller supplies DNS/proxy/TLS policy.
- Driver stop wakes physical I/O and cancels scoped internal RPC, authentication,
  heartbeat and token tasks. Join waits for physical thread and task-owned
  resources. EOF and failed handshakes also end the task scope. Already-resolved
  replies get one final poll so normal CloseOk still completes its caller.
- Async authentication provider hooks support native interaction without blocking
  physical I/O. Cancellation and provider failure explicitly resolve handshake
  waiters. Existing synchronous providers use the same async call path.
- Replace the unbounded socket-wakeup queue with a one-slot notification and
  independent atomic read/write readiness flags. Concurrent wake storms coalesce.
- Remove frame payloads and SASL challenge contents from frame tracing.
- Correct the upstream local retry fixture's accepted-socket blocking mode on
  macOS, where the listener's nonblocking mode is inherited.

Verified on macOS with Rust 1.97.0, no default features and the tokio feature:

```
cargo test --no-default-features --features tokio --lib \
  --test supplied_driver --test connection_retry
cargo clippy --no-default-features --features tokio --lib \
  --test supplied_driver --test connection_retry -- -D warnings
```

24 tests passed: 13 unit, 8 supplied-driver lifecycle/authentication, and 3
upstream retry/half-close fixtures. Clippy passed. The retry fixtures verify
upstream opt-in recovery; supplied-stream tests separately verify no retry.

Required before production replacement: bounded frame/content allocations,
request/event/consumer queue admission and retained-delivery accounting, complete
private authentication erasure/evidence isolation, explicit child resource drop
policy, Runtime action/result and dispatch evidence hooks, both declared field
value dialects, real RabbitMQ interoperability, and platform acceptance.
No main-project dependency should use this checkpoint as proof of those items.
