# Runtime-controlled transport and bounded delivery

Upstream: https://github.com/amqp-rs/lapin, tag v4.11.0,
base `0dc84fc1f6736817b75f7699e18ea2b08d3ed45f`.
Boundless patches are maintained on `boundless`; original upstream history is retained.

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
- Boundless disables Registry collection with `with_topology_collection(false)`;
  its journal owns structural restoration. Consumer payload queues retain native
  delivery reservations. Upstream automatic recovery remains opt-in and unchanged.
- The caller bounds supplied connector attempts. Pending publications and
  transactions are never replayed. Channel generation invalidation is synchronized
  with frame admission; stale acknowledgement/publication handles cannot enqueue work.
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
binding growth path a no-op and topology extraction empty.
This includes bindings to externally managed exchanges and queues that were
never declared through the connection. The retired topology budget cannot turn
a successful broker result into a metadata retention refusal.

The flag is authoritative even with automatic recovery enabled: recovery then
restores no exchanges, queues or bindings. This combination is accepted.
QueueDeclareOk still returns the broker's queue name and counts, and ConsumeOk
still establishes live consumer routing. Consumers still recover when upstream automatic recovery is enabled. Boundless's pin and connection construction change in its later work;
this patch changes only the fork.

Registry regressions inspect private storage, cloned registries,
external bindings and declarations in both modes. Channel regressions dispatch
protocol replies through the expected-reply machinery, check caller results and
a routed delivery, and inspect recovery snapshots with collection disabled in
both collection/automatic-recovery builder orders. These tests require no running RabbitMQ instance.

Regression: `cargo test --locked --no-default-features --features tokio --lib -- registry::tests channel::`.
All 8 tests pass (exit 0) on macOS arm64, including the existing channel
authentication and partial-get regressions. The new tests establish empty
storage, preserved broker results and routing,
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

## Retire local two-stage topology restoration and queue remapping (Rule 23)

Remove the restoration extension in `8dc9c39`, not its later correctness fixes.
That commit's `src/channel.rs:389-513` and `src/channels.rs:274-289` added
connection-wide prepare/finish passes; `src/registry.rs` retained
`server_generated` provenance and remapped queue names, `src/consumer.rs`
made queue names mutable, and `src/events.rs` added `QueueRecovered`.
Boundless's W1b cutover disables native recovery and topology retention; Rule 23
assigns symbolic names and restoration to its W7d journal instead.

Ordinary opt-in native recovery returns to upstream v4.11.0's single
`Channel::start_recovery` path: reopen, confirm mode, exchanges and bindings,
queues and bindings, then consumers using retained names. No fork remapping
pass or event remains. QueueDeclareOk still returns actual broker facts and
ConsumeOk still installs live consumer routing. Registry collection mode remains
private and every insertion still checks it. Promise completion, partial-get
cancellation, confirmation reset, receive reservations, explicit close ownership
and the W4d/W7d hooks are preserved.

Regression: `native_recovery_uses_retained_queue_names_and_literal_bindings`
checks QueueDeclareOk facts, the retained queue name used by recovery and literal
binding keys. Against `8420aab` it fails (exit 101: empty requested name instead
of the retained name). After removal it passes (exit 0); only its recovery-call
adapter changes from the deleted prepare/finish pair to `start_recovery`.
The existing disabled-Registry storage and QueueDeclareOk/ConsumeOk delivery
regressions are adapted to the removed metadata/remap signatures. macOS arm64
validation through `build-slot`: Tokio suite, strict all-target Clippy, codegen
and both format checks pass (exit 0).

Boundless still has an inert `Event::QueueRecovered` match arm. Its landing owner
must remove that arm with the new pin; this fork commit does not edit Boundless.

## Split unused topology budget and recovery switches from 9556710 (Rule 23)

At `3d3d385`, `src/connection_properties.rs:72-78,144-152` exposed
`with_topology_limit` and `with_recovery`; `src/configuration.rs:108-141`
constructed a second Budget and three recovery flags, and `src/registry.rs:27-59`
counted retained metadata after successful protocol replies. W1b disables native
recovery and Registry collection in Boundless. These unused local controls do not
own the W7d journal's admission or restoration and are removed, together with
metadata estimators, resizing, fallible Registry returns and budget-associated
binding deduplication. Registry's collection gate and private storage remain.

Kept from the mixed patch: Rules 10/11/19 delivery/frame reservations and their
release ownership, bounded event/RPC queues, erasure, generation admission and
explicit close ownership. All later promise, partial-get and close corrections,
and W4d completion/W7d freeze, remain. Native recovery configuration and
`Channel::start_recovery` now match upstream v4.11.0, apart from preserved
correctness patches and Registry's optional collection gate.

Regression evidence on macOS arm64, through `build-slot`:
- `successful_topology_replies_are_independent_of_delivery_capacity` exercises
  QueueDeclareOk, exchange declaration and both binding reply paths with zero
  receive capacity. On `3d3d385`, the baseline fixture additionally selects the
  soon-removed zero topology limit and fails (exit 101: metadata reported as a
  MessageBytes refusal after broker success). After removal that obsolete fixture
  builder is deleted; all four replies complete and their topology remains
  available (exit 0). The test maintains ordinary result/capacity independence,
  not a removed API shape.
- `upstream_recovery_is_opt_in_and_preserves_explicit_backoff` verifies default
  recovery off/zero retries, opt-in recovery with 16 retries and explicit backoff
  preserved in either builder order (exit 0).
- `upstream_recovery_restores_the_existing_consumer_and_tag` drives native
  recovery through OpenOk and ConsumeOk, then receives on the original consumer
  with the retained queue/tag and replacement generation (exit 0).
- Complete Tokio fork suite (76 tests), strict all-target Clippy and normal plus
  generated-source format checks pass (exit 0). The retained QueueDeclareOk/
  ConsumeOk, reservation, queue-bound, completion, partial-get and freeze
  regressions are included. No new fork API is added.

## Retain complete peer Close facts on existing errors (plan W5d, Rule 18)

At `234fb729`, `src/channel.rs:1093-1115,1179-1190` converted native
Connection.Close and Channel.Close through `AMQPError::try_from`. In the pinned
amq-protocol `83c33d2`, `protocol/src/protocol.rs:23-27,62-77` retains only
recognized reply codes and text in `AMQPError`; both conversions discard
class/method identifiers.
Unrecognized or successful connection closes became InvalidConnectionState(Closed),
and channel closes used normal cancellation without retaining any peer fields.
Upstream lapin v4.11.0 `src/channel.rs:946-969,1033-1044` uses the same
conversions. Its existing Error kind/accessor
and private shared notifier, and h2's accessors over its private error data, were
inspected before choosing this additive mechanism.

`Error::remote_close() -> Option<&RemoteClose>` exposes either
`RemoteClose::Connection(protocol::connection::Close)` or
`RemoteClose::Channel(protocol::channel::Close)`. The generated receivers capture the complete method before validation or
conversion, then pass the immutable capture into the native handler. Private shared error metadata is attached at
`Error::with_remote_close`, which preserves an already captured Close across
clones, notifier attachment and shutdown boundaries. Connection/channel status
keeps that metadata for the existing state-error fallbacks, including normal
cancellation, connection-wide closing and late operations. Child statuses refer
to their parent's connection status, so the closing transition cannot bypass the
capture while asynchronous shutdown is pending. Serialized-frame completions and
the I/O error return retain it too. A replacement handshake clears connection
metadata; channel generation changes and recovery finalization retire its status
metadata while errors already returned keep their original immutable Close.

No ErrorKind payload, equality, display/source, AMQP soft/hard classification,
recovery decision or lifecycle changes. In particular, unrecognized/successful
channel Close still emits no new consumer error or Event::Error. The enum's
variant is the received scope, even when a reply code's soft/hard class differs.
All four peer fields are verbatim, including zero class/method identifiers.
Boundless W5d must read the accessor before matching ErrorKind, add class_id and
method_id to Amqp.RemoteError, and take scope from the received method. Peer text
belongs to that response rather than an inferred cause or classification.

Regression evidence on macOS arm64, through build-slot with CARGO_INCREMENTAL=0:

- On `234fb729`, the native dispatcher regression with only an accessor scaffold
  returning None fails the raw Close retention assertion (exit 101). The scaffold
  adds no capture or propagation; both native Close handlers are unchanged in
  that before run. With this patch it passes (exit 0).
- `connection::remote_close_tests` drives the actual frame dispatcher and internal
  RPC owner for success, unknown, known soft and known hard codes in both scopes.
  It checks pending RPCs, partial basic.get, confirms, consumer/event errors,
  connection admission and channel fallback errors, plus unchanged normal channel
  cancellation and an unaffected sibling channel. An abandoned native handshake
  completes to its W4d sink with the complete Close. A status checkpoint test
  checks that replacement generations cannot inherit retired peer facts while
  the original error keeps them.
- Complete Tokio suite against owned RabbitMQ 4.3.5: 60 unit tests, 19 integration
  tests and 7 doctests pass (86 total, exit 0). Strict all-target Clippy, code
  generation and both crate/generated-source format checks pass (all exit 0).
  The generated Close receivers match the updated template; regeneration is
  repeatable.

The fresh W5d review found three uncovered paths at `7a13dfd1`:
`src/generated/channel.rs:1329` validated channel zero before capture;
`src/channel.rs:1555` rejected canceled authentication with a bare ConnectionAborted;
`src/io_loop.rs:268` enriched a clone at shutdown while returning a bare fallback.
The Close-specific generator metadata now captures both native Close types before
channel/state validation, retaining the same ProtocolViolation or state error.
All three authentication phases share a status-aware guard: explicit failure and
cancellation attach the recorded Close before resolver rejection and return.
Connector failure, handshake EOF and exhausted-backoff errors attach it before
propagation and driver return. Their error kinds and retry/close decisions stay.

The class search also reproduced missing fields when a Close targets a nonexistent
channel and when queued settlements run after Close removes their channel.
`Channels::receive_method` now retains either native Close on routing failure;
`invalid_channel_error` reuses the existing frame poison or connection capture for
removed-channel errors (including RPC lookup, repeated removal and header/body
fallbacks). No tombstone or extra retention map is added. Connection terminal
boundaries retain a Close supplied by their error too, so late errors keep a
channel-scoped Close that ended the connection, without reclassifying its scope.

Checked construction and return paths in the amended source:
- `src/error.rs:123,132,304`: the accessor and idempotent attachment preserve the
  error kind; ordinary constructors still start without inferred Close data.
- `templates/channel.rs:212` and `src/generated/channel.rs:1329,1638`: capture
  before both Close validators; handler capture passed explicitly.
- `src/channel.rs:904,1004,1042,1557`: start/continue/finish authentication guard;
  finish and Drop preserve the rejection kind and captured native fields.
- `src/io_loop.rs:218,244,270`: connector return, EOF and backoff fallback;
  attach before cloning, rejecting or returning.
- `src/channels.rs:113,122,143,171,183` and `src/internal_rpc.rs:440`: missing
  native-channel routing, removal and RPC/header/body fallback errors now retain
  actual Close facts; `src/frames.rs:220` reads existing channel poison.
- `src/channels.rs:189,196,212,308,348` and `src/connection_status.rs:80,92`:
  connection closing, closed/error/recovery/shutdown fan-out and late state errors.
- `src/channel.rs:176,194,204,796,811,881,1161,1226` and
  `src/channel_status.rs:126,298,314,333`: explicit channel fallbacks,
  connection-process rejection, close completion, recovery cause and generation
  retirement preserve metadata. Existing state/context and notifier semantics stay.
- `src/io_loop.rs:361,380,518,562,579,610,625,697`: critical I/O errors,
  serialized completions, state fallback, EOF, serialization, dispatch, parsing
  and Drop flow through the enriched boundary before public failure returns.
- `src/connection.rs:53,92,115,281,565` and `src/thread.rs:36`: stop, stop handle,
  driver join, Connection.run, handshake start and thread join either use the
  enriched terminal boundary or forward its exact Result; no new join error.
- `src/internal_rpc.rs:408,420,436`, `src/task_scope.rs:85,93`,
  `src/promise.rs:158,178,189,193,233` and `src/frames.rs:114,181,192,198,325`:
  RPC forwarding, scope cancellation, last-resolver fallback and frame/reply
  completion preserve typed errors; authentication's detached resolver is guarded.
- `src/internal_rpc.rs:265,339,344,465,480,495` and
  `src/frames.rs:38,45,61`: rejected/abandoned queued commands and generation skips
  retain actual poison facts before dropping a resolver. The existing poison map
  is shared independently of frame buffers, without a second map or an ownership
  cycle. `PromiseResolver::retain_remote_close` records metadata only; last-resolver
  drop keeps its BrokenPipe kind, message and State::complete ownership.
- `src/basic_get_delivery.rs:13`, `src/acknowledgement.rs:57,176`,
  `src/consumers.rs:96,102`, `src/consumer.rs:268,279,288`,
  `src/returned_messages.rs:53` and `src/error_holder.rs:17`: get, confirms,
  consumers, abandoned results and retained errors pass the original Error along.
- `src/channel_status.rs:29`, `src/acker.rs:120`, `src/heartbeat.rs:129`,
  `src/secret_update.rs:51` and `src/promise.rs:127`: stale-generation/admission,
  poisoned-settlement success, heartbeat, refresh logging and repoll failures are
  distinct local paths; no Close is fabricated. Heartbeat terminal errors use
  the same enriched connection boundary. Pre-I/O configuration/URI and native
  frame-limit/parser failures remain their existing independent errors.

Review regressions on `7a13dfd1`: nonzero-channel Close and real supplied-stream
async-auth Close fail (exit 101), with both handshake and driver errors checked.
The routing/state and queued-settlement regressions also fail before (exit 101).
After repair all eight dispatcher tests and the supplied-stream regression pass
(exit 0); the latter checks ordinary shutdown, missing-channel routing in both
native scopes and EOF, and joins all owners before comparing fields. ProtocolViolation, InvalidChannel, channel state and
IOError(ConnectionAborted) assertions protect the original classifications.

A further class regression on intermediate `fabaadcb` reproduces last-resolver
loss after a complete basic.get, received Close and RPC exit (exit 101). The
same test now passes after joining and dropping the driver-side fixture, leaving
only the retained message's settlement handle. The resolver checkpoint test
checks two resolver clones, an abandoned receiver/sink, one completion and the
unchanged BrokenPipe kind. Final macOS arm64 validation through build-slot:
60 unit + 19 integration + 7 doctests (86), strict all-target Clippy, fmt and
repeatable template generation all pass (exit 0). Queue admission, capacity,
stop behavior, retry decisions and error classification are unchanged.

## W5d receive observer (work in progress)

Received connection/channel Close is captured once before rejection, poison or
shutdown effects. The immutable ReceivedClose envelope carries the observer's
opaque context through status, errors and completion paths. Observer selection
is fixed by method scope; per-channel observers are installed before Channel.Open.
Configuration is immutable after installation. Callbacks run outside native locks.
These checkpoints use private envelope fields, first-write status setters and the
single generated receive entry, rather than changing close classification.

macOS arm64 regression evidence on observer work based on 51805ee0:
`cargo test --test remote_close_observer -- --nocapture` exited 101: four
observer regressions passed; `idle_connection_close_notifies_before_shutdown`
and `connection_close_context_precedes_rejection_effects` failed reading required
Connection.CloseOk with UnexpectedEof. The existing shutdown/frame poison defect
is tracked as Boundless #369; this patch does not repair it or weaken either test.
`cargo test --lib connection::remote_close_tests::first_received_close_envelope_is_retained_by_status_and_error`
exited 0 (one passed). `cargo build --features codegen-internal` and generated-file
formatting exited 0. All compile/test commands ran through build-slot.
This is WIP evidence; remaining native checks and Boundless integration follow.

Follow-up evidence on the same observer implementation: `cargo test --lib`
exited 0 (61 passed); `cargo test --features tokio --test supplied_driver`
exited 0 (11 passed). `cargo test --workspace` exited 101 after the 61 unit
and conn_refused tests passed: the existing connection integration requires
RabbitMQ and failed with Connection refused (os error 61). No broker was
configured for this run. `cargo test --test remote_close_observer` was repeated:
exit 101, four passed and the same two #369 CloseOk reads failed with
UnexpectedEof (early eof), at test helper frame-header read. The rejected-channel
subcase remains unreached after its accepted control fails. No tests are ignored.
Strict `cargo clippy --lib --tests --locked -- -D warnings` first exited 101
for the new accessor's missing must_use; after adding it, exit 0. Final
`cargo fmt --all --check` exited 0 after formatting the new fixtures. Commands
ran through build-slot on macOS arm64. Code was first committed as 52245298;
this follow-up changes only formatting, must_use and this evidence.

Current verification disposition (macOS arm64, observer tip
8a82380866770af95512327432122f4e52642e24): the RabbitMQ workspace integration
run is NOT RUN without a configured broker, per the landing owner. The earlier
attempt and its connection-refused result above are historical evidence, not a
passed gate. The observer's two Connection.CloseOk tests remain unchanged and
blocked by #369; the four other observer tests, 61 library tests, 11 supplied
driver tests, seven doctests, strict Clippy and formatting retain their recorded
results. No shutdown/frame-poison repair is included.

Additional W5d rejection controls (macOS arm64, based on f0d2cde5): a test-only,
nonblocking close-dispatch probe sits immediately before close_connection in
reject_connection_frame. The private probe records class/method ids; no native
shutdown order or production observer API changes. The wrapper control checks
one unchanged ProtocolViolation/503 with its original text and ids 70/20.
The received nonzero Connection.Close control installs distinct connection and
child observers, checks accepted=false and no dispatch inside the callback,
then checks retained context and exactly one dispatch with ids 10/50. The child
observer is never selected. These controls need no broker or Connection.CloseOk.
`build-slot cargo test --lib --locked channel::topology_collection_tests::`
exited 0 (eight passed). New unnecessary qualifications were then removed;
`build-slot cargo clippy --lib --tests --locked -- -D warnings` exited 0, and
`build-slot cargo fmt --all` exited 0.
`build-slot cargo test --lib --test remote_close_observer --test supplied_driver --features tokio --locked --no-fail-fast`
exited 101: all 63 unit tests and 11 supplied-driver tests passed; four observer
tests passed and the same two Connection.CloseOk tests failed at the required
frame header read with UnexpectedEof (early eof), then JoinError::Panic. Their
assertions remain unchanged. The accepted control still prevents that existing
integration test reaching its nonzero rejected subcase; the new independent
native unit control covers rejection before effects. #369 remains unmodified.
RabbitMQ workspace integration remains NOT RUN without a broker.
