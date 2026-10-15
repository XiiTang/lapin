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

## This client's own protocol closes (`ErrorKind::ProtocolViolation`)

lapin reported both a Channel.Close or Connection.Close the broker sent and a
close lapin sent itself, because the broker broke the protocol, as
`ErrorKind::ProtocolError`. A caller could not tell the broker's refusal from
lapin's verdict on the broker. lapin's three own closes (a connection frame on
a channel, invalid content and an acknowledgement for a delivery tag nothing
was published with) now return `ErrorKind::ProtocolViolation` carrying the
close lapin sent; a received close stays `ProtocolError`, and `is_amqp_error`
still means only that. Recovery treats both as before. Regression:
`cargo test --no-default-features --features tokio --lib a_close_for_the_brokers_violation`,
which fails on the previous commit with the acknowledgement's error reported as
`ProtocolError`. With it the validation above runs 27 tests (16 unit).

## Reject the last confirmation waiter before resetting tags (#63, batch 4)

`Acknowledgements::reset` now rejects pending confirmations while the old delivery
sequence still identifies the last waiter, then resets the sequence. Otherwise
`wait_for_confirms` loses its error resolver and never sees the recovery cause.
The checkpoint is `Inner::reset`; all acknowledgement resets go through it.

Regression: `cargo test --locked --no-default-features --features tokio --lib reset_rejects_the_last_waiter`.
The unchanged fork fails the last waiter's result assertion (exit 101); the patch
passes (1 test, exit 0) on macOS arm64. The test also confirms tag 1 is reused
only after pending waiters end and its new acknowledgement succeeds. Upstream's
acknowledgement reset and futures-channel's sender-drop completion were inspected.

## Complete a promise once, including abandoned resolvers (#63, batch 4)

Only resolver handles count toward liveness; the last unresolved resolver
rejects with a BrokenPipe error and wakes the waiter. `Shared` keeps completion
separate from its takeable result, so neither a late resolver nor resolver drop
can replace an already consumed result. Explicitly rejected causes win over the
unknown abandoned-resolver fallback. All resolver creation, clones, completion
and drops use the same locked state. This follows futures-channel's independent
completion flag and sender-drop wakeup, inspected alongside upstream Promise.

The StartOk and SecureOk callers now complete their step promises at frame-write
completion, while their separate overall-handshake resolver remains pending.
Their shared template and generated code change together: otherwise the newly
observable lost resolver turns an ordinary handshake into a connection error.

Regressions:
- `cargo test --locked --no-default-features --features tokio --lib promise::tests`:
  the original fork fails the wakeup and permanent-completion assertions (exit
  101; 1 passed, 2 failed); the patch passes all 3 (exit 0).
- `cargo test --locked --no-default-features --features tokio --lib authentication_step_waiters`:
  the old generated caller fails its post-write completion assertion (exit 101);
  the patched caller passes for both StartOk and SecureOk (exit 0).
- `cargo test --locked --no-default-features --features tokio --test connection_retry`:
  all 3 tests pass (exit 0), including protocol-header retries and normal auth.

Validation is on macOS arm64. Ordering observes wakeup, frame-write completion
and the separate handshake result, without sleeps in the new tests.

