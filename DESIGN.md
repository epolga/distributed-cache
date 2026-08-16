# DESIGN.md

## Wire protocol

One flat JSON message type (`WireMessage`) is reused for both client↔node and
primary↔replica traffic, length-prefixed on the wire (4-byte big-endian length + UTF-8
JSON payload — see `Protocol.cs`). `Kind` selects meaning: `SET`/`DEL`/`GET` are client
requests, `RESPONSE` is the reply, `HELLO` is a replica announcing its last applied
sequence number when a replication connection opens, `REPLICATE` is one applied write
streamed from primary to replica. A single DTO was chosen over polymorphic
deserialization because the assignment explicitly doesn't require a compact/typed wire
format, and it keeps the codec to ~15 lines.

Every write (`SET`/`DEL`) applied on the Primary is assigned a strictly increasing
`Seq` (a per-primary counter, not per-key). That sequence number is the backbone of
both guarantees below.

## What a client can rely on

- **Read-your-writes, not linearizability.** `SetAsync`/`DeleteAsync` return the write's
  `Seq`. Passing that `Seq` as `minSeq` to `GetAsync` on *any* node makes that node wait
  (bounded by a timeout, default 3s) until its local applied sequence reaches at least
  `Seq` before answering. A client that does this will never observe a state older than
  its own write. A client that does **not** pass `minSeq` gets whatever that node has
  applied so far — possibly stale on a replica that hasn't caught up yet, and there is
  no way to distinguish "stale" from "never written" in that mode.
- **No cross-client ordering guarantee.** If client X writes and client Y — who never
  learned X's `Seq` — reads a replica, Y can still see the old value. The guarantee is
  per-writer, not cluster-wide monotonic reads.
- **A write acknowledged by the Primary is durable only in memory.** No fsync, no disk
  log. If the Primary process dies, acknowledged writes not yet replicated are gone
  (see "left out" below).
- **`SET`/`DEL` sent to a Replica are rejected** with `NOT_PRIMARY`, not silently
  forwarded — a client must know it's talking to the Primary for writes.

## Concurrency

Each accepted client TCP connection is handled by its own task, reading/writing frames
sequentially (in-order request/response per connection). On the Primary, `Append` to the
replication log, `store.Set/Delete`, and advancing the applied-sequence gate happen
under one lock (`_applyLock`) so that "assigned Seq N" and "store reflects Seq N" become
visible atomically to a waiting `GET`. On a Replica, the same lock guards applying an
incoming `REPLICATE` message, with a dedup check (`seq <= AppliedSeq → skip`) so
re-sent/duplicate messages are safe to apply twice.

The store itself is a `ConcurrentDictionary`, so plain reads at a Replica never block on
the apply lock; only a `GET` carrying `minSeq` can wait, and only until that specific
sequence is reached.

## Backpressure

Two distinct places:

1. **Client connections.** A `SemaphoreSlim(50)` bounds how many client connections are
   actively being served per node. A burst of connects beyond that queues on the
   semaphore rather than spawning unbounded reader tasks.
2. **Replication.** The Primary does **not** push writes into a per-replica queue at
   all. Each `ReplicaLink` background task sends log entries to its replica strictly
   sequentially, `await`-ing the TCP write. If a replica (or the network) is slow, that
   `await` simply doesn't complete — TCP's own send-buffer backpressure stalls the
   sender. There is no unbounded application-level buffer that grows while a replica
   lags; the only thing that grows is the replication log itself, which exists
   specifically to support resend-on-reconnect (see below) and is bounded by total
   writes, not by any one replica's slowness. Client writes to the Primary are never
   blocked by a slow replica — the write returns as soon as it's applied locally, and
   replication happens on the independent `ReplicaLink` task.

## Convergence & degraded operation

Every replication connection starts the same way regardless of whether it's a brand-new
replica or a reconnect after a drop: the Replica sends `HELLO{AppliedSeq}` first, and
the Primary streams `log.From(AppliedSeq)` — i.e., "everything you don't have yet,"
which is a superset that may include entries the replica already applied if its report
is stale relative to what was in flight. The Replica's dedup check
(`seq <= AppliedSeq → skip`) makes re-applying those safe. This means "replica catches
up after connecting late" and "replica resumes after a dropped connection" are the exact
same code path — there's no separate reconnect-repair logic to get wrong.

If a `ReplicaLink`'s TCP connection throws (refused, reset, EOF), the loop backs off
~300ms and retries indefinitely. There's no cap on retry attempts (this is a fixed
3-node topology with no failover — a permanently dead replica just never catches up,
which is an accepted simplification, not a monitored/alerted condition).

## What was deliberately left out

- **No persistence.** The replication log lives only in the Primary's memory; a Primary
  restart loses all history and every replica would need to be considered stale.
  Real fix: append the log to disk (or hand it to a real log-structured store) so the
  Primary can restart without replicas re-syncing from scratch — explicitly out of scope
  per the assignment.
- **No cap on replication log growth.** It grows for the process's lifetime. For a
  long-running cluster you'd want either periodic compaction (once all replicas have
  acked past some point, older entries are safe to drop) or a real log store. Not
  implemented — would add real complexity (tracking each replica's ack point) for a
  scope that explicitly excludes eviction/TTL policy design.
- **No liveness/health signal for a stuck `GET minSeq`.** A `GET` with `minSeq` blocks
  up to the fixed 3s timeout, then returns `STALE_TIMEOUT`. There's no retry-with-backoff
  helper on the client side — a caller that wants that has to loop itself.
- **Retry budget on `ReplicaLink` is unbounded and unmonitored** (see above) — fine for
  a fixed 3-node topology, not something I'd ship without a circuit breaker / alert in
  a real system.
- **No way to add a replica to a running Primary.** `_replicaTargets` is fixed at
  construction. The catch-up mechanism itself needs no change to support this (a new
  replica's `HELLO` is already handled like a reconnect), so this is a narrow,
  additive gap, not a redesign — left out because the assignment excludes "dynamic
  cluster discovery."
- **`CacheClient` doesn't auto-reconnect** after a broken connection — the caller has
  to construct a new one. Not needed by any of the five tests; a real client would
  detect the failure and reconnect transparently.
- **No connection limiter on the replication-accept side**, unlike the client-facing
  side (`_connectionLimiter`). Harmless on the fixed loopback topology; would matter
  on an open network.
- **`Seq` is visible to the caller** (`SetAsync`/`DeleteAsync` return it, `GetAsync`
  takes it as `minSeq`), rather than hidden behind a server-tracked client session.
  Considered and rejected as unnecessary complexity for an ergonomics-only change —
  full reasoning in `KeyDecisions.md`.

## Testing notes

Five behavioral tests (`DistributedCacheTests.cs`):
`HappyPath_SetThenGetOnPrimary_ReturnsWrittenValue`,
`Set_SentToReplica_IsRejectedWithNotPrimary`,
`ReadYourWrites_GetOnReplicaWithMinSeq_NeverObservesStaleValue`,
`ConcurrentWrites_ToPrimary_ConvergeOnAllReplicas`,
`LateJoiningReplica_CatchesUpOnHistoryWrittenBeforeItConnected`.

These are integration/behavioral, not unit tests — every one spins up real `Node`s
over real TCP. Deliberate: the assignment asks for "meaningful behavioral tests" over
"broad line coverage." `SeqGate`/`Frame`/`ReplicationLog` are pure and cheap to
unit-test in isolation (no network, no timing); I'd add that separately given a larger
test budget, but within five tests I spent the budget on the claimed system-level
guarantees instead.

The last one is how "resend after reconnect" gets exercised without killing a live
socket mid-stream: the replica's listener simply isn't started until after the Primary
has already written 25 entries, so the Primary's `ReplicaLink` spends that time retrying
a refused connection — by the time it succeeds, it's in exactly the state a real
reconnect would be in (Replica reports `AppliedSeq=0`, Primary resends everything).

**Awkward scenario I didn't script:** actually killing an established TCP connection
mid-stream (after the replica has applied *some* but not all in-flight entries) and
asserting it resumes from the right point. It's exercised indirectly — the dedup logic
that makes it safe is the same code path covered by the late-join test and by
`ConcurrentWrites_*` (which has concurrent in-flight replication while writes continue).
Scripting the literal socket-kill would mean reaching into `TcpClient` internals from
the test or adding a test-only fault-injection hook to `Node`, which felt like more
surface area than the guarantee needed to be trusted. With more time I'd add a small
seam (e.g. an injectable `Func<Task>` the `ReplicaLink` awaits between sends) purely for
this test.
