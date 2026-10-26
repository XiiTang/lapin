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

## End partial basic.get deliveries with their channel (#63, batch 4)

`BasicGetDelivery::cancel` takes the partial message once, releases its receive
reservation and rejects its resolver with the channel's known cause. Every
channel close, connection error, recovery initialization and recovery reset
calls that checkpoint. Completed messages keep their result and reservation
until the recipient drops them. Upstream content assembly and consumer cleanup
were inspected; this uses their existing take-and-reject ownership pattern.

Regression: `cargo test --locked --no-default-features --features tokio --lib partial_get_tests`.
On the preceding fork the terminal-path test fails because closing leaves the
get pending (exit 101); completed delivery preservation already passes. With
the patch all 3 tests pass (exit 0) on macOS arm64. Tests cover clean and error
closing, closed, connection error, recovery and reset, repeated cancellation,
first-cause retention, and both partial and completed receive-budget ownership.
## Keep publisher-confirm fixtures on the existing Delivery interface (#63 lint)

The publisher_confirms example and its symlinked integration test now build
expected deliveries through `Delivery::mock`, setting their existing public
properties and acker fields. The private receive reservation stays owned by the
constructor. The socket-state wake-storm test initializes its readiness flags
with its state, removing field_reassign_with_default without weakening the lint.

Regression/check: `cargo clippy --locked --no-default-features --features tokio --all-targets -- -D warnings`.
Before this fixture-only patch it fails (exit 101) on both Delivery struct
literals and the socket-state initializer; afterwards it passes (exit 0), on
macOS arm64. The existing library wake-storm test also passes. No broker test
is claimed executed by this compile/lint check, and no lint was suppressed.

## Disable duplicate topology collection at Registry (protocol plan W1b)

The protocol plan's W1b cutover disables lapin automatic recovery and duplicate
topology collection. Boundless's journal owns restoration once W7d adds explicit
reconnect.
`ConnectionProperties::with_topology_collection(bool)` defaults to `true` for
existing callers. Setting it to `false` makes every Registry declaration,
binding and queue-remapping growth path a no-op and topology extraction empty.
This includes bindings to externally managed exchanges and queues that were
never declared through the connection. Disabled collection reserves no topology
budget, so a successful broker result cannot fail because of metadata retention.

The flag is authoritative even with automatic recovery enabled: recovery then
restores no exchanges, queues or bindings. This combination is accepted.
QueueDeclareOk still returns the broker's queue name and counts, and ConsumeOk
still establishes live consumer routing. Consumer recovery remains separately
configured. Boundless's pin and connection construction change in its later work;
this patch changes only the fork.

Registry regressions inspect private storage and reservations, cloned registries,
external bindings and queue remapping in both modes. Channel regressions dispatch
protocol replies through the expected-reply machinery, check caller results and
a routed delivery, and inspect recovery snapshots with collection disabled in
both recovery-builder orders. These tests require no running RabbitMQ instance.

Regression: `cargo test --locked --no-default-features --features tokio --lib -- registry::tests channel::`.
All 8 tests pass (exit 0) on macOS arm64, including the existing channel
authentication and partial-get regressions. The new tests establish empty
storage with no limit or a zero limit, preserved broker results and routing,
empty exchange/queue recovery snapshots, and unchanged default collection.
`cargo clippy --locked --no-default-features --features tokio --lib -- -D warnings`
and `cargo fmt --all -- --check` also pass (both exit 0).

## Refresh locked certificate verification dependencies (#312)

Update the fork lockfile's rustls-webpki 0.103.15 to 0.103.16 and
webpki-root-certs 1.0.9 to 1.0.10, the smallest patched releases for
RUSTSEC-2026-0382 and RUSTSEC-2026-0383. They reject distrusted emSign
intermediates and remove the affected bundled trust anchors. The manifest's
compatible dependency constraints and lapin behavior are unchanged.

Regression coverage: the Tokio-only lapin library suite and Boundless's
AMQP module, with the main lockfile resolving the same fixed TLS packages.

## Keep the advisory-fixed AMQP source in the pin closure (#312)

Pin amq-protocol 83c33d2497d84653a5ce1ad0402d8d702bf940db in Cargo.toml
and Cargo.lock. That fork refreshes its own webpki and root-certificate locks;
its protocol implementation is unchanged. The webpki 0.103.16 and roots
1.0.10 resolutions above remain fixed. Regression coverage: the complete
Tokio-enabled library suite and Boundless's complete AMQP library tests.

## Report the receive or queue bound that was exceeded (#201, W11e)

`ResourceLimitExceeded` previously discarded which check failed, so an embedding
application could report a single oversized body, a retained delivery count, an
event queue or an internal RPC queue as retained body bytes. It now carries a
`ResourceLimit`, the actual maximum and the attempted total when representable.
`Budget::reserve` checks one body's advertised bytes before aggregate bytes and
delivery count; queue owners supply their own count bounds. Reservation ownership,
queue sizes and non-recoverability are unchanged. Topology collection is outside
this patch.

`IoLoop::parse` reports an oversized peer frame as `FrameTooLarge`, retaining
its whole advertised size and the receiving maximum. This keeps a malformed peer
response distinct from local resource exhaustion, including the negotiated-frame
check after parsing. Boundless maps these errors only at `amqp::Native::lapin`.

Regressions (macOS arm64): `cargo test --locked --no-default-features --features
tokio --lib limits::` checks the exact three body/delivery limits and release;
`cargo test --locked --no-default-features --features tokio --lib queue_reports`
fills both real queues and checks their exact count failures. Boundless's
`oversized_content_is_rejected_before_body_allocation` checks the body's own limit;
`an_oversized_peer_frame_is_an_invalid_response_before_its_body_arrives` sends only
an over-limit frame header and checks the program-visible peer failure.

## Acknowledge a peer's Connection.Close before retiring the transport (#369)

Shutdown drops pending frames and poisons the queue; recovery poisons channel
zero too. Both then submitted Connection.CloseOk through the poisoned queue,
so the peer read EOF. Reordering submission alone would still let shutdown drop
the queued acknowledgement. `Frames::Inner::push` is the private admission
checkpoint: only Connection.CloseOk on channel zero passes either poison;
ordinary work keeps the original rejection cause. Bulk content admission stays
poisoned. No poison is cleared and no retry or recovery policy changes.

The single SendConnectionCloseOk command waits for the existing frame-write
promise before calling the retirement hook with its original error. Its
before_connection_close_ok hook releases Connection.Blocked's write hold only
after shutdown/recovery has rejected ordinary queued work; otherwise an alarmed
peer's close deadlocks behind that hold. The generated method no longer retires
at queue admission; templates/lapin.json and regenerated src/generated/channel.rs
agree. This prevents non-200 closes from marking I/O errored before CloseOk is
written. The command is the retirement hook's sole caller.

Frames::take_connection_shutdown_reply retains the whole authentication-step or
Open expected reply, including its cancellation resolver, until that same write
ends. Rejecting a handshake earlier lets its caller stop the owned driver before
CloseOk. Cancellation then follows the existing ExpectedReply cancellation
checkpoint with the original error; ordinary RPCs still reject at shutdown.
The optional retained reply is boxed so the bounded internal command queue does
not carry a 400-byte enum in every slot (strict Clippy catches that regression).
No new completion or sending path is introduced. ConnectionStatus::blocked's
comment documents the closing handshake's release of the hold.

RabbitMQ Java client's AMQConnection.handleConnectionClose and
AMQChannel.quiescingTransmit were inspected: shutdown rejects application work
while its acknowledgement still transmits before final shutdown.

Regressions on macOS arm64:
- `cargo test --no-default-features --features tokio --locked --test connection_close`:
  f34c12f reads EOF in all three initial owned-peer cases (exit 101).
  The patch covers completed handshake with heartbeat zero, code 200 with
  recovery off; code 320 rejecting a pending declaration with its original
  cause; and code 320 through recovery initialization with zero retry attempts.
- The same target adds a blocked peer and a gated CloseOk write during Open.
  The gated test fails on the admission/retirement-only repair (exit 101, EOF)
  when its failed-handshake caller stops the driver; with deferred rejection
  it decodes CloseOk before returning the original 530 cause. The ordinary
  cases also check that CloseOk is the last and only close response.
- `frames::close_ok_tests` checks the private admission checkpoint for both
  poisons, original Arc cause rejection, and admission versus write completion.

Validation (all exit 0): `cargo test --lib --features tokio --locked` (31 tests);
Tokio-only `connection_close`, `connection_retry` and `supplied_driver` (5, 3 and
8 tests); Tokio-only library build and strict Clippy (`--lib --tests -- -D
warnings`); and rustfmt checks of every touched Rust file. `cargo fmt --check`
also passed before the final handshake/blocked cases; those added files are
included in the final per-file check. Broker-dependent tests, including
tests/connection.rs, are not claimed run.
This repair changes no Boundless API contract or observer mechanism.
