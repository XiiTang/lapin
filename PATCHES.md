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

HandshakeResolver is the private completion owner from ProtocolHeader through
OpenOk, including auth_starter_async, continue_auth_async and finish_auth_async.
ConnectionStatus holds only a weak handle, so it neither keeps an abandoned
handshake alive nor cycles through a completed Connection. On peer Close this
owner reserves the original cause before choosing shutdown or recovery and
before either rejects ordinary work. Queued reply cancellation,
AuthenticationGuard completion and cancellation all pass
through it; auth errors cannot send a competing retirement command while CloseOk
is pending. The same SendConnectionCloseOk command releases the owner after the
write attempt ends. No queued-reply retention path remains. Ordinary RPCs still
reject at shutdown, and the ordinary Promise completion latch is unchanged.
ConnectionStatus::blocked's comment documents the closing handshake's release
of the hold.

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
- The asynchronous-auth regressions hold each of the starter, continuation and
  finish arms, then hold the actual CloseOk write. Releasing authentication
  (both success and error) leaves the outer handshake pending. Releasing the
  write lets the peer decode CloseOk and completes once with the original 530
  peer cause. On the reviewed 1e71e38 baseline all three auth-arm tests read
  EOF (exit 101). A shared-owner unit test checks reservation, late auth errors,
  dropping an auth resolver and permanent completion directly.
- `frames::close_ok_tests` checks the private admission checkpoint for both
  poisons, original Arc cause rejection, and admission versus write completion.

Validation (all exit 0): `cargo test --lib --features tokio --locked` (32 tests);
Tokio-only `connection_close`, `connection_retry` and `supplied_driver` (8, 3 and
8 tests); Tokio-only library build and strict Clippy (`--lib --tests -- -D
warnings`); and rustfmt checks of every touched Rust file. `cargo fmt --check`
also passed before the final handshake/blocked cases; those added files are
included in the final per-file check. Broker-dependent tests, including
tests/connection.rs, are not claimed run.
This repair changes no Boundless API contract or observer mechanism.

## Settle a rejected frame's local close before retiring I/O (#378)

Base: `966311590693c4b44bb0db0b9d7b105c650f3020`, without W5d observers.
A rejected peer frame could lose its requested local Close or have its original
`ProtocolViolation` replaced by the I/O owner's `ConnectionAborted`. The receive
path queued CloseConnection (or a channel-close future) and SetConnectionError;
critical_error stopped I/O before those commands and their write ran. Poison
then rejected the Close, or Drop canceled its RPC scope first. This predates
#369; its CloseOk-only poisoned admission stays unchanged.

Instances searched: all nine connection-method channel-zero checks (Start,
Secure, Tune, OpenOk, Close, CloseOk, Blocked, Unblocked and UpdateSecretOk);
invalid heartbeat and channel-zero content header; unexpected replies, content
headers, content classes and bodies through report_protocol_violation; and
invalid single Ack/Nack through acknowledgement_error. All three native verdict
construction paths now use Channel::reject_frame. The multiple Ack/Nack
complete_pending_before loop cannot return its error while it owns the pending
map; an empty set succeeds, so that is not a reproduced rejection instance.
Automatic connection/channel cleanup, caller-requested closes, parse/size
errors and invalid connection states do not return ProtocolViolation after
requesting a local close and are outside this class.

The checkpoint is shared ConnectionStatus shutdown ownership. Channels::handle_frame
holds its private serialization guard before entering any channel receive lock;
Channel::reject_frame reserves the original verdict there, before queueing the
write handoff. All Channels connection close, error, shutdown poison and recovery
transitions use the same guard. Driver stop and IoLoop::drop enter through
Channels::set_connection_error, so neither can publish ConnectionAborted first.
The private RPC actor no longer owns a separate rejection flag. An already queued
SetConnectionClosed, local close, peer Close, error or explicit stop can abandon
the attempt only with the original verdict. Explicit stop abandons immediately.
Public RPC-queue callers also pass through InternalRPCHandle::await_reply: lost
resolvers and an RPC scope that has finished retiring resolve against the same
reserved verdict. Already-ready replies propagate before cancellation.
Closing admission changes channel state without canceling a partial basic.get.
Normal input is discarded while rejecting; a later Close cannot run native
teardown against the retained verdict. Reads remain active to observe EOF.
Handshake rejection reuses #369's HandshakeResolver, released only when the write
settles or is abandoned. A local Channel.Close remains channel-scoped; its
original verdict still ends the connection. Blocked writes are released for the
required Close. Frames::Inner::push and CloseOk-only poison admission are unchanged.

The write attempt races a ten-second runtime timer. Lapin has no close or
handshake deadline setting to reuse (ConnectionProperties, configuration, TCP
entry points and handshake/RPC paths were searched). Its negotiated heartbeat
may be zero and is canceled during closing, so it cannot bound this attempt.
The private constant gives an already rejected connection a finite best-effort
write without adding a public knob. It is a local policy choice, not a duration
copied from another client. Successful poll_write means transport acceptance,
not proof of peer delivery. Timeout, transport failure, peer EOF and competing
shutdown retain the original ProtocolViolation; none substitutes a timeout or
ConnectionAborted end. A rejected connection also skips potentially pending
transport poll_close during physical retirement, and exits the outer handshake
retry loop even when a caller configured connection backoff. An admitted end
cannot become a second connection attempt.

Source precedent: rabbitmq/amqp091-go `connection.go` at
`30d99a718190e4dbd6c77d030b22b6b4fbf33b94`, closeWith defers shutdown until its
close exchange returns; dispatch0 sends CloseOk before shutting down its writer.

Regression evidence: macOS arm64, Rust 1.98.1, this patch on the base above.
Commands below ran through the machine's build-slot wrapper, with incremental
compilation disabled and the checkout's own target directory.

- Before the fix: `build-slot cargo test --no-default-features --features tokio
  --locked --test connection_close
  rejected_connection_close_is_written_before_the_protocol_violation_retires_io
  -- --exact`: **101**, actual wire read failed with UnexpectedEof, `early eof`.
  The original verdict happened to win that run; the write was still lost.
- Baseline `build-slot cargo clippy --no-default-features --features tokio --lib
  --tests --locked -- -D warnings`: **0**.
- Fixed `build-slot cargo test --no-default-features --features tokio,smol
  --locked --test connection_close`: **0**, all 11 tests, including unchanged
  #369 CloseOk/blocked/auth/recovery regressions, the actual Close 503, partial
  basic.get, blocked/unblocked rejection, handshake write failure, other content
  and reply rejections, and single Ack/Nack Close 406.
- Two further runs of `build-slot cargo test --no-default-features --features
  tokio,smol --locked --test connection_close
  rejected_connection_close_is_written_before_the_protocol_violation_retires_io
  -- --exact`: **0**, **0**. Together with the module run, three final regression
  runs; each exercises all four pending-Declare/partial-Get × blocked/unblocked
  schedules and proves a held write cannot finish the original waiter early.
- `build-slot cargo test --no-default-features --features tokio,smol --locked
  --no-fail-fast`: **0**, 61 unit/integration tests and 7 doctests. All broker
  tests ran against an isolated RabbitMQ 4.3.5 with owned ports, data and process
  PIDs; no tests were skipped or ignored. This includes
  frames::close_ok_tests and the shared handshake-owner regression.
- `build-slot cargo build --no-default-features --features tokio,smol --locked`:
  **0**.
- `build-slot cargo clippy --no-default-features --features tokio,smol
  --all-targets --locked -- -D warnings`: **0**, no new or baseline findings.
- `build-slot cargo fmt --all --check`: **0**.

The private checkpoint unit regression
internal_rpc::tests::frame_rejection_keeps_its_verdict_until_the_close_write_settles
covers successful and failed writes, queued ConnectionAborted, SetConnectionClosing,
InitConnectionShutdown and a SetConnectionClosed queued before the write handoff.
Each arm retains the original verdict and publishes one end.
An initial added fixture incorrectly expected multiple Ack/Nack of an empty set
to reject and timed out (portable module command **101**); correcting that
fixture to exercise actual rejection paths passed the narrow rerun (**0**) and
the full suite above. No product assertion was weakened.

### #378 review corrections (F1–F3), 2026-10-27

Source precedents read before the correction:
- amqp091-go connection.go at `30d99a718190e4dbd6c77d030b22b6b4fbf33b94`,
  CloseDeadline/setDeadline apply a caller-supplied socket deadline before the
  close exchange; closeWith defers shutdown until that exchange returns. Its
  30-second default is a connection/handshake timeout, not a close-write bound.
- rabbitmq-c amqp_api.c/amqp_socket.c/amqp_connection.c at
  `8b7471eab8d09536b3c104dbb30a65699cf48104`: connection_close uses simple_rpc,
  whose optional rpc_timeout bounds wait_frame_inner, but simple_rpc_inner calls
  amqp_send_method with an infinite write deadline. This is not a bounded close
  write precedent: the lower amqp_send_method_inner/send_frame_inner APIs accept
  explicit deadlines, but connection_close does not supply one to the write.
  A null RPC timeout is infinite; its separate default login timeout is 12 seconds.
  Go's socket deadline is the applicable bounded-write precedent; neither client
  supplies a reusable lapin setting or a close-write constant to copy.

Regression evidence: Darwin arm64, Rust 1.98.1, this correction on
`f30dd76a77e09d483e5555f7c21a3d6510b8be0a`. Every cargo invocation below
used build-slot, CARGO_INCREMENTAL=0 and the checkout's own target.

- With the previous production sources restored (new fixture and Tokio test-util
  only), `cargo test --no-default-features --features tokio,smol --locked
  --test connection_close rejected_close_ -- --nocapture`: **101**, all five
  new regressions failed. Pending write, peer EOF and later peer Close hit the
  fixture's bounded completion assertion. Explicit stop produced IOError /
  ConnectionAborted instead of ProtocolViolation 503. Local close returned the
  wrong end kind/code. No fixture schedule or assertion was changed for this run.
- Fixed `cargo test --no-default-features --features tokio,smol --locked
  --test connection_close`: **0** three consecutive final runs, 16 tests each.
  The five new tests cover permanently pending writes (paused clock advanced
  explicitly), peer EOF, local close with partial Get, peer Close and explicit
  stop while the write is held. They use controlled gates, not sleeps. Existing
  wire tests retain Close codes and end kind/code, with only necessary write /
  waiter ordering. Exact text, full diagnostics and empty-tail assertions removed.
- `cargo test --no-default-features --features tokio,smol --locked --lib
  frame_rejection_keeps`: **0**, expanded shared-checkpoint regression.
- `cargo test --no-default-features --features tokio,smol --locked
  --no-fail-fast`: **0**, 66 unit/integration tests and 7 doctests, none ignored;
  isolated RabbitMQ 4.3.5 with owned ports/data/PIDs. The first full run was
  **101** because the old classification fixture sent a peer Close after a
  rejection on the same connection. The independent peer-Close arm now uses a
  fresh connection; its classification assertions are unchanged.
- `cargo build --no-default-features --features tokio,smol --locked`: **0**.
- `cargo clippy --no-default-features --features tokio,smol --all-targets
  --locked -- -D warnings`: **0**, no baseline or new warnings.
- `cargo fmt --all --check`: **0**.

### #378 narrow re-review: cancelled public RPC callers (F2), 2026-10-27

Class: a public caller awaiting a queued/scoped RPC reply returns resolver-loss
BrokenPipe, or remains pending on an unprocessed command, after a protocol
rejection has already reserved the connection's end. Merely guarding the actor's
CloseConnection handler cannot govern a command that is never processed.

Source inventory and instances:
- Connection::close -> close_connection_checked: SetConnectionClosing can retire
  the scope before the CloseConnection resolver is constructed/enqueued.
- Connection::create_channel and Connection::update_secret: their single queued
  command owns the caller's resolver; queue or spawned-task retirement can lose it.
- Acker::ack/nack/reject: all use the one private Acker::rpc funnel, with the same
  resolver ownership after its initial error check and before the reply returns.
- Channel::close and Channel::basic_cancel do not enqueue multiple actor commands:
  their frame and expected-reply registration use one Frames lock. Retirement
  rejects registered replies; later pushes see the original poison. Other
  generated method replies use that same frame path. Publisher confirmations
  are retained in the acknowledgement map and rejected by channel error cleanup.
  They are not instances of this RPC-command cancellation class. Fire-and-forget
  cleanup has no public caller promise; handshake completion keeps #369's owner.

Checkpoint and enforcement: all public RPC-queue waits use the private
InternalRPCHandle::await_reply entry, including Acker's single private RPC
funnel. The handle carries the same ConnectionStatus as Channels, not a new end
owner. Resolver-loss BrokenPipe and a pending reply after full scope retirement
are projected to that owner's reserved ProtocolViolation. With no reservation,
ordinary cancellation retains BrokenPipe. Completed successes and other completed
errors are preserved. The existing stopped Event also wakes retirement listeners
when its last task guard drains; listening before checking the state prevents a
missed stop, including a caller arriving after retirement. Waiting for the drain
keeps TaskScope's existing final-poll rule: already-ready replies propagate before
external cancellation. No new timeout, result registry or close path is added.

Source precedent: amqp091-go connection.go at
`30d99a718190e4dbd6c77d030b22b6b4fbf33b94`, Connection.call reads the buffered
shutdown reason when the RPC channel closes, rather than replacing that reason
with generic channel closure. Lapin's existing TaskScope::wrap likewise polls a
ready reply once before dropping stopped work.

Regression evidence: Darwin arm64, Rust 1.98.1, this patch on
`87741d17e90e8ba05cc8ce3ad8e79c8a5162cfb4`. All cargo commands use build-slot,
CARGO_INCREMENTAL=0 and this checkout's target.

- Before the production fix, `cargo test --no-default-features --features
  tokio,smol --locked --lib local_close_between_commands_keeps -- --nocapture`:
  **101**, returned IOError/BrokenPipe (lost last resolver), not ProtocolViolation
  503. A test-only Notify gate pauses immediately after SetConnectionClosing;
  the controller witnesses the reserved verdict, fully retires the RPC scope,
  and only then resumes the caller before CloseConnection enqueue.
- Before the fix, `cargo test --no-default-features --features tokio,smol
  --locked --lib queued_rpc_callers_keep -- --nocapture`: **101** with the
  deterministic readiness assertion: retirement left the queued caller pending.
  An initial version awaited that pending reply; its owned process tree was
  recorded and terminated (**143**). The bounded reproduction uses a single
  state probe, not a sleep or timeout, and keeps the required end assertions.
- Fixed `cargo test --no-default-features --features tokio,smol --locked --lib
  internal_rpc::tests::`: **0** three final runs, five tests each. This includes
  the inter-command gate, six queued public-call routes, and a control that a
  ready scoped reply completes successfully before cancellation. The existing
  rejection/write and queue-limit checkpoint tests also pass.
- `cargo test --no-default-features --features tokio,smol --locked
  --no-fail-fast`: **0**, 69 unit/integration tests and 7 doctests, none ignored,
  using isolated owned RabbitMQ 4.3.5. #369's poisoned admission and CloseOk
  regressions remain unchanged.
- `cargo build --no-default-features --features tokio,smol --locked`: **0**.
- `cargo clippy --no-default-features --features tokio,smol --locked --all-targets
  -- -D warnings`: **0**.
- `cargo fmt --all --check`: **0**.

### #378 reply precedence and abandonment provenance (narrow review r2), 2026-10-27

Class: a completed RPC result is replaced by a reserved shutdown verdict, either
when its producer completes between the promise and retirement polls, or when a
real completed BrokenPipe is mistaken for resolver abandonment. The six callers
and frame-path exclusions inventoried above are unchanged.

Checkpoint: InternalRPCHandle::await_reply rechecks the promise after observing
full TaskScope retirement. The retirement state lock synchronizes with the last
producer guard's release; the recheck therefore preserves its completed success
or error before substituting for an unresolved reply. Promise's private State
stores Completion::Completed(Result<T>) or Completion::Abandoned(Error). Only
last-resolver Drop writes Abandoned, under the existing state mutex. The RPC
checkpoint substitutes the reserved verdict only for that proven abandonment
or an unresolved reply after retirement. A completed genuine BrokenPipe stays a
completed I/O error. Public Error/ErrorKind and Promise's ordinary Future output
are unchanged; outside this checkpoint abandonment still becomes the same
BrokenPipe as before. There is no new public error, owner or cancellation path.

Source precedent: futures-channel 0.3.31 src/oneshot.rs represents sender loss as
Canceled separately from a sent value; a Receiver<Result<T, E>> thus distinguishes
abandonment from a producer's completed error. The change retains lapin's existing
arm-before-recheck listener loop and releases the state lock before notification.

Regression evidence: Darwin arm64, Rust 1.98.1, this patch on
7c0337164983e2320ffb417c248b2833a5272247. Every Cargo command uses build-slot,
CARGO_INCREMENTAL=0 and this worktree's target.

- Baseline `cargo test --no-default-features --features tokio,smol --locked --lib
  internal_rpc::tests::rpc_ -- --nocapture`: **101**, the success and ordinary
  error completed between polls and the completed genuine BrokenPipe all fail;
  each is replaced by ProtocolViolation. The abandoned-resolver arm passes on
  the baseline as a positive control. A test-only synchronous hook executes the
  ready producer and releases its final guard after the first pending promise
  poll and before retirement is observed, with no sleep.
- Fixed `cargo test --no-default-features --features tokio,smol --locked --lib
  internal_rpc::tests::`: **0**, three final runs, nine tests each; all four new
  cases and the existing inter-command, queued-caller and ready-reply controls.
- `cargo test --no-default-features --features tokio,smol --locked --lib
  promise::tests::`: **0**, three existing promise completion/abandonment tests.
- `cargo build --no-default-features --features tokio,smol --locked`: **0**.
- `cargo fmt --all --check`: **0**.
- `cargo clippy --no-default-features --features tokio,smol --locked --all-targets
  -- -D warnings`: **0**, no warnings.
- `cargo test --no-default-features --features tokio,smol --locked
  --no-fail-fast`: **0**, 73 unit/integration tests and seven doctests, zero
  ignored, with isolated owned RabbitMQ 4.3.5. #369's regressions are unchanged.
## Preserve native completion and abandoned confirm ownership (plan W4d)

Rule 7 requires the owner of an untaken native answer to retain its disposition.
At the preceding fork revision `f34c12f`, `src/promise.rs:24-30` only logged receiver
drop; `Shared::set` stored an answer in private receiver state. Dropping an outer
RPC/get future could therefore discard a completed result or its later answer.
`src/returned_messages.rs:126-150` flattened abandoned Ack/Nack values to returned
messages, losing their distinction and errors, and `src/acknowledgement.rs:176-183`
cleared abandoned confirms when the channel failed.

The native receiver is now public as `Completion<T>`. Its `with_sink`,
`close_and_take` and `try_take` transfer the actual typed result. Receiver drop
closes and takes under the same private state mutex used by `State::complete`;
that checkpoint owns explicit success/error and last-resolver-drop completion.
Only the receiver or its sink can take an answer. The sink runs once, outside
the completion mutex; it must hand work to the embedding owner's queue without
blocking or reentering lapin. A consumed answer cannot be completed again.
Tokio 1.53.3's oneshot `Receiver::drop` (close then consume), `Sender::send`
(return an untaken value) and futures-channel 0.3.31's sender-drop completion
were inspected in their source before implementing this handoff.

Channel RPCs expose synchronous `*_completion` admission methods generated from
the same template as their unchanged lazy async entry points, including
`basic_get_completion` and `basic_publish_completion`. The publish write's
completion hands over its `PublisherConfirm`; `PublisherConfirm::into_completion`
then transfers the broker-confirm receiver. `Channel::take_dropped_confirms`
transfers abandoned native receivers, pending or ready, preserving Ack, Nack,
NotRequested, errors, returned bodies and reservations. The legacy explicit
`wait_for_confirms` drain retains its returned-message API. Connection RPCs,
supplied-stream/connector handshakes and Acker RPCs expose the same typed native
receiver through their `*_completion` entry points. Pre-admission errors are
returned directly; the sink owns only an admitted operation's untaken result.
Internal RPC forwards its native receiver directly to the caller's resolver,
without an intermediate task poll that could lose a committed reply at stop.
This covers channel creation, connection close, secret update and Acker replies;
maintenance tasks keep their existing owner and scheduling.

Regression evidence (macOS arm64):

- `cargo test --locked --no-default-features --features tokio --lib
  dropped_confirms_retain_ack_nack_and_error_for_their_owner` with only the test
  added to `f34c12f` fails its retained-error assertion (exit 101); the patch
  preserves all three outcomes both before and after receiver drop, and also
  NotRequested.
- `promise::tests` forces send-before-drop, drop-before-send, close-then-take,
  concurrent send/drop, explicit error and last-resolver loss. It verifies one
  handoff and that the sink runs outside the completion mutex.
- `channel::completion_tests` drives actual expected-reply dispatch, complete
  get bodies and receive-budget release, empty/error get results, Ack/Nack with
  returned bodies, and publish-write completion handing off its confirm.
- `abandoned_native_handshake_completes_to_its_owner_on_stop` drives a supplied
  transport, abandons the native handshake receiver, stops and joins the physical
  owner, and observes its one error completion with transport destruction.

The crate's Tokio suite runs against an owned isolated RabbitMQ 4.3.5, including
the upstream broker tests, retry and supplied-driver tests and doctests.
Clippy checks all targets with `-D warnings`; formatting checks the crate and
the separately generated channel source. No Boundless adapter or pin changes
belong to this phase-0 patch; W14c consumes the confirm handoff.
The complete suite passes 40 unit tests, 17 integration tests and 7 doctests
(exit 0); Clippy, both formatting checks and code generation pass (exit 0).

## Freeze physical ingress with final channel delivery tags (plan W7d, fork part)

Rule 23 needs a final old-generation high-water mark before replacement data is
admitted. At W4d revision `02ed755`, `src/channels.rs:187-239` dispatched methods,
headers and bodies without a fence. A caller could neither stop that admission
atomically nor account for tags already retained in native consumer/get buffers.

`ConnectionDriver::freeze_ingress() -> BTreeMap<ChannelId, DeliveryTag>` is an
irreversible native frame fence. `Channels::handle_frame`, the I/O loop's one
dispatch entry point, holds the same private mutex as freeze through dispatch,
content assembly and receiver completion. Each Basic.Deliver and Basic.GetOk
updates its channel's maximum before dispatch, including no_ack, buffered and
partial messages. Publisher-confirm tags do not enter this map. Entries survive
native channel removal and candidate failure. A channel absent from the map has
mark zero; repeated freezes return the same values, without offset arithmetic.
Creating a new native channel lifetime on a reused id resets that id's mark
under the same fence before its first delivery. This prevents an ended
channel's tag 13 from inflating a replacement's actual high-water tag 1; a
regression reproduces that wrong snapshot before the reset. Creation after
freeze cannot change the retirement snapshot. Automatic recovery is disabled
by the embedding caller when the owner represents one physical generation.

Complete results already accepted remain takeable from their consumer queues and
get receivers with the original generation, tag, body and reservation. Later
frames are not dispatched, so a partial body cannot become a complete delivery
after the fence. Freeze itself sends no settlement/close and ends no feed. The
retirement owner takes the snapshot, stops and joins the driver, then admits
replacement data; it can also freeze an already stopped/failed candidate.
Stop/error and resolver-drop completion still use W4d's completion checkpoint.
Checked cumulative offsets and captured settlement belong to Boundless W7d;
structured close remains W5d's separate patch.

Source precedent inspected before implementation: RabbitMQ Java client's
[ConsumerDispatcher.quiesce](https://github.com/rabbitmq/rabbitmq-java-client/blob/main/src/main/java/com/rabbitmq/client/impl/ConsumerDispatcher.java)
separates new work admission from queued work, and
[RecoveryAwareChannelN](https://github.com/rabbitmq/rabbitmq-java-client/blob/main/src/main/java/com/rabbitmq/client/impl/recovery/RecoveryAwareChannelN.java)
keeps the maximum seen delivery tag separately from the active offset. This
patch uses lapin's existing frame dispatcher, includes get tags, and leaves
logical offsets and recovery policy to its caller.

Regressions exercise the actual dispatcher with two consumers and get on one
channel, a second independent channel, no_ack, unpolled complete bodies and
GetOk, distinct confirm tags, a pending late GetOk, partial bodies, a failed
candidate, removed/reused native channels and u64::MAX. A blocking test waker establishes an
in-flight handoff; the test observes the dispatch mutex held and joins freeze
before checking its snapshot and later-delivery refusal, without a scheduling
sleep. The supplied-driver test freezes before handshake and after stop, then
checks the original aborted-handshake error and transport destruction.

The before test uses unchanged production code at `02ed755` and a test-only
no-op stand-in for the not-yet-existing freeze API. Its assertion checks that a
complete post-fence delivery cannot reach the consumer; the final tests use
the real hook, with no stand-in.
On macOS arm64 that before test fails the post-freeze delivery assertion
(exit 101). With this patch all six dispatcher tests pass. The complete Tokio
suite against owned RabbitMQ 4.3.5 passes 46 unit tests, 18 integration tests
(including the public supplied-driver freeze test) and 7 doctests (exit 0).
`cargo clippy --locked --no-default-features --features tokio --all-targets --
-D warnings`, `cargo fmt --all -- --check` and the generated-source rustfmt
check also pass (exit 0).

## Retire automatic QoS snapshots and replay (owner audit, Rule 23/W7d)

Remove the local extension from `04c282a`: at `0b6c75f4`,
`src/channel_status.rs:59-70` retained confirmed QoS, `src/channel.rs:536-601`
replayed it during recovery, and `src/consumer.rs:137` retained per-consumer
prefetch. Boundless disables native automatic recovery and Registry collection
under W1b; its W7d journal owns restoration. Ordinary `basic.qos`, including its
native completion receiver, remains unchanged. The QoS reply now completes at
the ordinary generated reply dispatcher without a snapshot hook. Consumer
creation and all recovery paths have no local QoS snapshot to read.

Source comparison: upstream v4.11.0 `src/channel.rs:377-463` restores consumers
without these snapshots or QoS replay. The shared generator metadata and generated
reply variant return to that behavior while retaining W4d completion ownership.

Regression: `qos_replies_complete_without_adding_recovery_work` dispatches and
acknowledges both global and non-global QoS, then checks that an otherwise empty
recovery has no further QoS work. On `0b6c75f4` it fails (exit 101); after removal
it passes (exit 0). Validation uses `build-slot`, `CARGO_INCREMENTAL=0`, on macOS
arm64: Tokio fork suite, strict all-target Clippy, generator build and both normal
and generated-source format checks (all exit 0).
