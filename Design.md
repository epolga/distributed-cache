# Design Notes

Working notes on non-obvious decisions in this codebase — what was chosen, what the
alternatives were, and why.

## `SeqGate`: polling instead of an event-driven wake

**Decision:** `SeqGate` tracks a monotonic value under a lock. A caller waiting for
that value to reach some target polls it on a short interval (20ms) until it does, or
until a timeout elapses.

```csharp
public async Task<bool> WaitForAsync(long target, TimeSpan timeout, CancellationToken ct)
{
    if (Value >= target) return true;
    var deadline = DateTime.UtcNow + timeout;
    while (true)
    {
        var remaining = deadline - DateTime.UtcNow;
        if (remaining <= TimeSpan.Zero) return Value >= target;
        var delay = remaining < PollInterval ? remaining : PollInterval;
        await Task.Delay(delay, ct).ConfigureAwait(false);
        if (Value >= target) return true;
    }
}
```

**Alternative considered:** an event-driven wake — a `TaskCompletionSource` held by the
gate, completed and replaced on every `Advance()`, with waiters `await`-ing it and
re-checking their own target on each wake (since one `Advance()` might not reach every
waiter's target, and a signal can carry no more information than "something changed" -
see the monotonicity argument below). This avoids any polling interval entirely and
wakes waiters the instant the value changes.

**Why polling instead:** the event-driven version is correct, but its correctness
depends on getting several subtle things right at once: a `TaskCompletionSource` must
be recreated (not reset) after each completion, since a `Task` can only complete once;
continuations must run with `TaskCreationOptions.RunContinuationsAsynchronously` or
risk executing inline inside the lock that `Advance()` holds; and waiters must always
re-check the *live* value on wake rather than trust any payload attached to the signal,
because a burst of rapid `Advance()` calls can make a signal's payload stale before a
delayed waiter ever processes it. None of that is exotic, but it's several independent
things that all have to be correct together for the primitive to behave as intended.

Polling sidesteps all of it. `WaitForAsync` only ever does "get the current value,
compare it to a number" — there's no continuation lifetime to reason about, and no
scenario where re-checking gives a wrong answer, because `Value` is always read live,
never cached. The cost is bounded and explicit: up to `PollInterval` (20ms) of added
latency per wait, and periodic wake-ups instead of none while idle. Against a
`ReadYourWritesTimeout` measured in seconds, 20ms is a rounding error.

**Why this still relies on monotonicity, not on catching every individual signal:**
`Advance(seq)` only ever moves `_value` forward
(`if (seq > _value) _value = seq;`), never back. That means the *current* value alone
is always sufficient to answer "has target been reached" — if `Value` is 25 and the
target was 20, the current value already proves 20 was passed, whether or not anything
observed the exact moment it happened. A signal-based design that skips a signal (or
gets a stale one) can't lose correctness for the same reason: it's an optimization
over "look at the live value," not a replacement for it.

**When the event-driven version would be worth it:** if wait latency needed to be
sub-millisecond, or if this were being polled by thousands of concurrent waiters where
even a cheap poll's aggregate cost matters. Neither applies here.

## `InMemoryStore` vs `ReplicationLog`: both grow unbounded today, but not for the same reason

**Current state:** neither `_store` nor `_log` has any active mechanism limiting its
size. `_store` only shrinks when a client explicitly sends `DEL` for a specific key -
there's no TTL, no eviction, no bulk clear. `_log` never shrinks at all; it's
append-only for the process's lifetime. Looked at purely as "does this grow forever
today," they're equivalent.

**Where they stop being equivalent: what it would take to fix each one.**

For `_store`, a size-limiting policy (TTL, LRU eviction, whatever) would be a
**local** decision - the store doesn't need to know anything about any other node's
state to decide "this key hasn't been touched in a while, evict it." Nothing outside
the store is affected by that decision.

For `_log`, removing an old entry is **not** a safe local decision. If an entry gets
trimmed before every replica has actually received and applied it - say a replica is
currently disconnected, or just slow - that replica has no way to ever get that entry
back. Its next `HELLO`+`From(afterSeq)` catch-up would silently skip straight past the
gap, and convergence breaks: the replica would never end up in the same state as the
Primary, with no error or signal that anything went wrong.

**What `_log` would actually need first, before any trimming policy could be added
safely:** a way for the Primary to know, for every replica, how far it has actually
applied - which doesn't exist today. The only thing a replica currently tells the
Primary is its `AppliedSeq` once, in `HELLO`, at connection time; there's no ongoing
acknowledgment while the connection is live. Only once the Primary can compute "the
minimum applied Seq across all currently-live replicas" would it be safe to trim
`_log` up to that point. So for `_store`, what's missing is just a policy. For `_log`,
what's missing is a whole communication channel (replica -> primary ACKs) that the
policy would sit on top of - trimming is the easy part once that exists.

### `_store` is the thing that actually gets replicated; `_log` never is

Worth stating plainly: `_store` is duplicated across nodes - that's the entire point
of replication. Primary applies a write to its own `_store`, records it in `_log`,
and streams it out; each Replica applies the same write to its *own*, separate
`InMemoryStore` instance via `ApplyReplicatedEntry`. The three nodes end up with three
independent `_store` objects that converge to the same contents. `_log`, by contrast,
is never copied anywhere - only the Primary ever has one (see the Node.cs role split:
`_log` is Primary-only, `_replicationListener` is Replica-only).

**Does `_store` being duplicated reintroduce the same coordination problem discussed
above, if a size-limiting policy were added to it?** Not if the policy lives only on
the Primary and evicts by sending an ordinary `DEL` through the existing replication
path, rather than having each node decide independently. Eviction then becomes just
another write flowing through the already-proven `_log`/`REPLICATE` pipe -
`_log.Append(WireOp.Del, ...)`, `_store.Delete(...)`, `_appliedSeq.Advance(...)`, the
same internals `HandleDelete` already uses, just triggered by a timer instead of a
client request. No new protocol, no new message type, no per-node coordination logic -
only a new *initiator* of an operation that already exists and is already replicated
correctly. The only residual lag is the same ordinary replication lag every write
already has (closeable with `minSeq` if it ever mattered for an evicted key, which it
wouldn't).

One real design choice this forces: TTL should be measured from **last write**, not
last read. The Primary sees every write (it's the only writer), but it has no
visibility into `GET`s served directly by a Replica - a read-based TTL would need the
Replicas to report access back to the Primary, which is a different, unbuilt channel.
Write-based TTL needs nothing new beyond what's described above.

## What happens if the Primary itself restarts

Nothing about the Primary survives a restart either - `_log`, `_store`, and
`_appliedSeq` are all in-memory only, same as everywhere else in this system. A fresh
`Node` starts with an empty log, an empty store, and `_appliedSeq = 0`. Replicas that
were already running keep whatever they had applied before the crash - they don't
lose anything themselves, since they didn't restart.

**This isn't just "replicas are now stale" - it's a concrete, reproducible correctness
bug via `Seq` collision.** When a Replica with, say, `AppliedSeq = 500` reconnects and
sends `HELLO{AppliedSeq: 500}`, the new Primary's `_log.From(500)` looks at its own
(empty) history:

```csharp
public IReadOnlyList<ReplicationEntry> From(long afterSeq)
{
    if (afterSeq >= _entries.Count) return Array.Empty<ReplicationEntry>();
    ...
}
```

`_entries.Count` is `0`, so `500 >= 0` is true, and it returns nothing - the new
Primary concludes "you're already caught up," when in reality it just has no history
at all, old or new.

The new Primary then starts accepting fresh writes and numbering them **from 1
again**. Those `REPLICATE` messages (`Seq = 1, 2, 3...`) reach the Replica, whose
dedup check sees:

```csharp
if (seq <= _appliedSeq.Value) return; // "already applied" - silently skipped
```

`1 <= 500` is true, so the Replica silently discards every new write from the
restarted Primary, genuinely believing it has already applied them - when in fact
these are entirely new writes that happen to carry Seq numbers from a range the
Replica already passed through in a *previous* incarnation of the Primary. The
Replica doesn't just lag; it permanently stops receiving new data after a Primary
restart, with no error anywhere.

**What a real fix would need:** `Seq` alone isn't enough to survive a Primary
restart - it needs to be paired with something that changes every time a new Primary
process starts (an "epoch" or "generation" number), so a fresh Primary's `Seq=1` can
never collide with a previous incarnation's `Seq=1` in a Replica's eyes. That epoch
value would itself need to come from somewhere durable across the Primary's own
restart (persisted to disk, or derived from wall-clock time at startup) - which loops
back to the same root cause as everything else in this file: nothing in this system
survives a process restart, anywhere.

## Walking `ReadYourWrites_GetOnReplicaWithMinSeq_NeverObservesStaleValue` end to end

Worth having one full trace written down, since this test is where every piece
(`Node`, `SeqGate`, `ReplicationLog`, `CacheClient`) actually meets in one place.

**Setup.** `replicaB` and `primary` (with `replicaB` as its one `ReplicaTarget`) are
constructed, then started. Starting `replicaB` launches its client-accept loop and
`AcceptReplicationLoopAsync`. Starting `primary` launches its client-accept loop and,
because `_replicaTargets` is non-empty this time (unlike the happy-path test),
`ReplicaLinkLoopAsync(target)`. That loop connects to `replicaB`'s replication port,
reads its `HELLO{AppliedSeq: 0}`, finds nothing to send yet (the log is empty), and
parks on `WaitForMoreAsync`. The `Task.Delay(200)` right after just gives that
handshake time to finish before the measured loop starts - it isn't part of the
guarantee being tested.

**One iteration of the loop, in full:**

1. `writer.SetAsync(key, val)` sends `SET` to Primary. `HandleSet` runs under
   `_applyLock`: `_log.Append` assigns `seq` and advances `_appendedGate`;
   `_store.Set` writes the value; `_appliedSeq.Advance(seq)` advances Primary's own
   gate. The response (with `Seq`) goes back to `writer`.
2. In the background, the moment `_appendedGate` advanced, the parked
   `ReplicaLinkLoopAsync` wakes, reads the new entry via `_log.From(...)`, and sends
   it to `replicaB` as a `REPLICATE` message.
3. `reader.GetAsync(key, minSeq: seq)` sends `GET` to `replicaB` with that same `seq`
   as `MinSeq`. `HandleGetAsync` sees `MinSeq` is set and calls
   `_appliedSeq.WaitForAsync(seq, ...)` - on the Replica's *own* gate this time, not
   Primary's.
4. Two possible timings here, both correct: if `replicaB`'s own
   `HandleReplicationConnectionAsync` loop already read and applied the `REPLICATE`
   message (via `ApplyReplicatedEntry`, which advances Replica's `_appliedSeq`) by
   the time `WaitForAsync` checks, it returns `true` immediately - no wait. If not,
   `WaitForAsync` polls (every 20ms) until that same `Advance` call satisfies it.
   Either way, once it returns, `_store.TryGet` is guaranteed to find the value,
   because the wait specifically guaranteed the write was already applied before the
   read happened.
5. Both assertions (`found`, and the value matches) pass as a direct consequence of
   step 4 - not because of timing luck.

**Why the loop runs 50 times with no sleep between write and read:** if the
`minSeq` wait were broken (a missing `await`, a race in `_applyLock`, whatever), it
would only fail *intermittently* - exactly when the replica happens not to have
caught up on its own by chance. A single iteration could easily pass by accident.
Fifty consecutive iterations, with no artificial delay giving replication extra time
to "naturally" catch up, make a broken guarantee show up reliably instead of
sometimes.

## Loopback is hardcoded in two different places, for two different reasons

```
src/DistributedCache.Core/Node.cs:65:   _clientListener = new TcpListener(IPAddress.Loopback, ClientPort);
src/DistributedCache.Core/Node.cs:75:   _replicationListener = new TcpListener(IPAddress.Loopback, ReplicationPort.Value);
src/DistributedCache.App/Program.cs:13: ReplicaTargets = new[] { new ReplicaTarget("127.0.0.1", 6101), ... }
```

**`Node.cs` - a real constraint in the library itself, not just the demo.**
`IPAddress.Loopback` means "only accept connections from this same machine." On a
real server, where clients or peer nodes live on other machines, connections
wouldn't even reach the listener. For real deployment this would need to become
`IPAddress.Any` (listen on every network interface) - this isn't a demo-only detail,
it's baked into `Node`'s constructor.

**`Program.cs` - a natural consequence of running everything in one process.** Since
all three nodes live in one process on one machine, `127.0.0.1` is the only address
that makes sense here. On three real machines these would be real IPs/hostnames
(`"10.0.1.12"`, `"replica-b.internal"`) - `TcpClient.ConnectAsync(host, port, ct)`
already resolves hostnames via DNS on its own, nothing extra needed there.

**What's honestly missing:** unlike a from-scratch real deployment would need, this
project has no per-machine entry point at all - `DistributedCache.App` is purely an
in-process demo. Actually running this on three machines would need two things:
switching `IPAddress.Loopback` to `IPAddress.Any` in `Node.cs`, and a separate `Main`
that builds a single `NodeConfig` from something external (environment variables, a
config file) instead of hardcoding all three nodes in one file.

## Can a Replica ever receive a `REPLICATE` before its own `HELLO` went out?

No - this is structurally impossible, not just unlikely, given how both sides are
sequenced plus TCP's own ordering guarantee.

**Replica side** (`HandleReplicationConnectionAsync`): the read loop doesn't start
until the `HELLO` write has fully `await`-ed, including `Frame.WriteAsync`'s explicit
`FlushAsync` - so by the time this code could possibly read anything, its own `HELLO`
bytes have already been handed to the transport, not just queued somewhere in memory.

```csharp
await WireCodec.WriteMessageAsync(stream, new WireMessage { Kind = WireKind.Hello, ... }, ct)...;
while (!ct.IsCancellationRequested)
{
    msg = await WireCodec.ReadMessageAsync(stream, ct)...; // only starts after the write above
    ...
}
```

**Primary side** (`ReplicaLinkLoopAsync`): the send loop doesn't start until `HELLO`
has been fully read.

```csharp
var hello = await WireCodec.ReadMessageAsync(stream, ct)...; // read HELLO first
long lastSent = hello.Seq ?? 0;
while (!ct.IsCancellationRequested)
{
    foreach (var entry in _log!.From(lastSent))
        await WireCodec.WriteMessageAsync(stream, new WireMessage { Kind = WireKind.Replicate, ... }, ct)...; // only now
    ...
}
```

**Putting the two together with TCP's ordering guarantee:** for Primary to have read
a `HELLO` at all, the Replica must have already sent it (TCP delivers bytes on one
connection in the order they were sent, never out of order). And Primary won't send a
single `REPLICATE` until that read completes. So the chain - Replica writes `HELLO` →
Primary reads it → Primary writes `REPLICATE` → Replica reads it - can't have a link
happen before the one before it, on either side, ever. Each connection also starts
this sequence from scratch, so a previous (now-dead) connection's in-flight bytes
can't leak into a new one either.

## Why framing specifically, and where it's actually solved

TCP is a byte stream, not a message stream - a single `Read()` is not guaranteed to
return exactly what a single `Write()` sent. Data can arrive split across several
reads, or several small writes can coalesce into one read. Assuming "one read = one
message" is a classic, easy-to-make networking mistake.

This is also the one requirement that would simply vanish if a higher-level transport
(gRPC, SignalR, HTTP) were allowed - those handle framing invisibly. Using raw
`TcpClient`/`Socket` is specifically what makes framing something to design at all,
which is why it's called out on its own rather than folded into "implement the
protocol."

**Where it's solved - two distinct problems, two distinct fixes, both in
`Protocol.cs`'s `Frame` class:**

1. **Partial reads.** `ReadExactAsync` loops on `stream.ReadAsync`, accumulating into
   a buffer until exactly the requested number of bytes has arrived, rather than
   trusting a single call to return everything:

   ```csharp
   private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
   {
       int offset = 0;
       while (offset < buffer.Length)
       {
           int read = await stream.ReadAsync(buffer.AsMemory(offset), ct).ConfigureAwait(false);
           if (read == 0)
               return offset == 0 ? false : throw new EndOfStreamException("Connection closed mid-frame.");
           offset += read;
       }
       return true;
   }
   ```

   `read == 0` mid-way (some bytes already collected) means the connection died
   inside a frame - a real error (`EndOfStreamException`), not a clean disconnect.

2. **Malicious/corrupt length.** Checked immediately after parsing the header,
   *before* allocating anything for the payload:

   ```csharp
   int length = BinaryPrimitives.ReadInt32BigEndian(header);
   if (length < 0 || length > MaxPayloadBytes)
       throw new InvalidDataException($"Frame length {length} out of bounds (max {MaxPayloadBytes}).");
   var payload = new byte[length];
   ```

   Without this, a corrupted or hostile length value would drive `new byte[length]`
   straight into an enormous (or, for a negative value, invalid) allocation before
   anyone gets a chance to reject it.

**One precision worth stating explicitly: the header and the payload behave
differently with respect to "length."** The header is *always* exactly 4 bytes -
that's fixed, because it's the size of the `Int32` length field itself, never
anything else. The payload's length is *not* fixed or repeated - it's read fresh
from each message's own header, so a tiny `GET` and a `SET` with a long value get
different-sized reads. `MaxPayloadBytes` (1 MiB) is not "the length we expect" - it's
a ceiling used only to reject unreasonable values; real payloads are almost always
far smaller than it.

## A garbage frame with a plausible length used to kill a connection silently

Framing (above) only guards the *length* prefix. It says nothing about whether the
payload bytes that follow are valid JSON. Two garbage cases turned out to be handled
very differently before this fix:

- **Length itself is garbage** (negative, or bigger than `MaxPayloadBytes`) - already
  caught by the bounds check in `Frame.ReadAsync`, which throws `InvalidDataException`.
- **Length is small and in-range, but the payload bytes aren't valid JSON** - nothing
  caught this. `JsonSerializer.Deserialize<WireMessage>` throws
  `System.Text.Json.JsonException`, which does **not** derive from `IOException`. Every
  existing error-handling call site (`HandleClientAsync`, `HandleReplicationConnectionAsync`
  in `Node.cs`) only catches `IOException`, so the exception fell through, the
  fire-and-forget task (`_ = HandleClientAsync(client, ct);`) became an unobserved task
  exception, and the connection died with no response sent and no trace logged. The
  `finally { _connectionLimiter.Release(); }` still ran, so the semaphore slot wasn't
  leaked - only the sender got silence instead of an error.

**The fix - one place, not several.** `Frame.ReadAsync` already throws
`InvalidDataException` for its own bad-length case, and `InvalidDataException` derives
from `IOException`. So `WireCodec.ReadMessageAsync` now catches `JsonException` and
re-throws it wrapped as `InvalidDataException`, matching the existing convention
instead of inventing a new one:

```csharp
public static async Task<WireMessage?> ReadMessageAsync(Stream stream, CancellationToken ct)
{
    byte[]? bytes = await Frame.ReadAsync(stream, ct).ConfigureAwait(false);
    if (bytes is null) return null;
    try
    {
        return JsonSerializer.Deserialize<WireMessage>(bytes, Options)
               ?? throw new InvalidDataException("Empty/invalid wire message.");
    }
    catch (JsonException ex)
    {
        throw new InvalidDataException("Malformed wire message.", ex);
    }
}
```

Fixing it here, at the one place both call sites already funnel through, means every
existing `catch (IOException)` block now handles malformed JSON the same way it already
handles a dropped connection - without touching `Node.cs` at all. No new exception type
for callers to learn, no new catch clause to duplicate at every read site.

## Why `WireMessage` is one flat DTO instead of several message types

**Decision:** a single class with seven fields (`Kind`, `Op`, `Key`, `Value`, `Seq`,
`MinSeq`, `Status`), all nullable except `Kind`. Which fields are meaningful depends on
`Kind` - documented in the doc comment above the class, not enforced by the type
system.

**Alternative considered:** a type per message shape (`SetRequest`, `GetRequest`,
`ReplicateMessage`, `HelloMessage`, ...), either as a polymorphic hierarchy with a JSON
type discriminator, or as separate envelope/payload pairs.

**Why flat:** at seven fields and six `Kind` values, a discriminator-based polymorphic
setup would add a real amount of machinery - a base type, a type-discriminator
attribute or manual envelope, a switch to pick the concrete type on deserialize - to
save nothing at the call sites, which already switch on `Kind` either way
(`HandleClientAsync`'s `request.Kind switch { ... }`). One type means one
`JsonSerializer.Deserialize<WireMessage>` call everywhere, no polymorphic
configuration to get wrong.

**The honest cost:** every message carries fields it doesn't use - a `REPLICATE`
message always has a null `Status`, a `GET` always has a null `Op`. At seven fields
this is a non-issue; it's the kind of thing that stops being fine if the protocol grew
many more `Kind` values each needing their own extra fields, since `WireMessage` would
turn into a large bag of nullable fields whose valid combinations are documented only
in a comment, not checked by the compiler.

## Why `Node` is one role-based class instead of `PrimaryNode`/`ReplicaNode`

**Decision:** one `Node` class with a `NodeRole Role` field, branching on it where
behavior actually differs (constructor, `StartAsync`, `HandleSet`/`HandleDelete`'s
`if (Role != NodeRole.Primary) return NotPrimary`). Primary-only state (`_log`,
`_replicaTargets`) and replica-only state (`_replicationListener`) both live on the
same class, just nullable/empty when not applicable to the current role.

**Alternative considered:** `PrimaryNode`/`ReplicaNode` inheriting from a common base
that holds the shared client-facing logic.

**Why one class:** most of what `Node` does doesn't depend on role at all -
`AcceptClientsLoopAsync`, `HandleClientAsync`, `HandleGetAsync`, `StartAsync`,
`StopAsync`, `DisposeAsync` run identically either way. Only the replication side
(inbound vs outbound) actually differs. Splitting by inheritance would leave two
derived classes that are mostly empty, with nearly everything pulled up into the base
- at that point the split isn't really doing anything except adding two extra types
and a factory to pick between them. Since role is decided by `NodeConfig` at
construction time, not by the compiler, something has to make that choice at runtime
regardless of whether it's a class-selecting factory or a field check - the role field
does the same job with less indirection.

**The Liskov angle:** a `Replica extends Primary` (or the reverse) relationship
wouldn't actually hold up. A Replica must reject `SET`/`DEL` with `NOT_PRIMARY` -
it can't be freely substituted anywhere a Primary is expected, which is exactly what
"extends" is supposed to promise. The real boundary being enforced here is a network
protocol rule (who's allowed to write), not a type relationship - `Role` being checked
inside `HandleSet`/`HandleDelete` reflects that directly instead of trying to encode a
substitutability guarantee that doesn't actually exist.

## Why `Get` is async but `Set`/`Del` are sync in `Node` - and all three are async in `CacheClient`

**On `Node` (the server side):** `HandleGetAsync` is `async` because it can genuinely
await something that takes time - `_appliedSeq.WaitForAsync(minSeq, ...)`, the
read-your-writes wait, bounded by `ReadYourWritesTimeout` (3s). `HandleSet` and
`HandleDelete` are plain synchronous methods: under `_applyLock`, they append to the
in-memory `_log`, write to the in-memory `_store`, and advance a `SeqGate` - three
in-memory operations, nothing to ever wait on. Making them `async` would add an
`async` state machine around code that never actually suspends.

**On `CacheClient` (the client side):** `SetAsync`, `DeleteAsync`, and `GetAsync` are
all `async`, without exception, because every one of them goes through `SendAsync`,
which does real I/O - write the request to a `NetworkStream`, then read the response
back. The client never touches an in-memory store directly; there's no local-only case
the way there is on the server. So the same "is there something to actually await"
rule that splits `Node`'s three handlers two-sync/one-async lands on all-async on the
client, because on that side, every operation is a network round trip.

## `SendAsync`'s write-then-read, and why `HandleClientAsync`'s loop has no matching write first

**`CacheClient.SendAsync`** writes the request and then reads exactly one response,
every call:

```csharp
await WireCodec.WriteMessageAsync(stream, request, ct).ConfigureAwait(false);
return await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false)
       ?? throw new IOException("Connection closed before a response arrived.");
```

This is safe from cross-talk because `_sendLock` (a `SemaphoreSlim(1, 1)`) guarantees
only one call is inside `SendAsync` at a time per connection - so the read right after
a write can never accidentally consume a different call's response; there is only ever
one request in flight to answer.

**`Node.HandleClientAsync`'s loop reads first, with no write before it**, because the
server side of this exchange is purely reactive - it never has anything to say until a
request arrives. The very first thing that can happen on a freshly accepted client
connection, from the server's point of view, is the client's request landing; there's
nothing to write in advance of that.

**Contrast with `HandleReplicationConnectionAsync`**, which *does* write first (its
`HELLO`) before entering its read loop - because on that connection the accepting side
(the Replica) is structurally the initiator of the handshake: it has to announce its
`AppliedSeq` before there's anything meaningful for the Primary to send back. Same
"write before read" question, opposite answer, because the two connections assign
different roles to whoever happens to be the one accepting the socket.

## Why the tests register disposal at construction (`await using var x = Create...(...)`)

**Current pattern**, in every test:

```csharp
await using var replicaB = TestHarness.CreateReplica("B", out _, out int replPortB);
await using var primary = TestHarness.CreatePrimary("A", out _, new ReplicaTarget("127.0.0.1", replPortB));
```

**Earlier pattern**, before this was changed: construct into a plain local first,
register it for disposal on a separate line afterward - e.g. `var replicaB = TestHarness.CreateReplica(...); ...; await using var d1 = replicaB;`.

**Why the change:** in the two-step version, there's a gap between "the node object
exists" and "its disposal is guaranteed" - if constructing a *later* node threw (a
port collision from `TestHarness.GetFreePort`, say) after an *earlier* node had already
been constructed but before that earlier node's own `await using var d1 = ...` line had
run, the earlier node would never get registered for disposal at all. It would leak -
its `TcpListener` stays bound, its background accept loop keeps running - because the
thrown exception unwinds straight past the line that would have protected it.
Registering disposal in the same statement as construction (`await using var x = Create...`)
removes that gap entirely: there's no window between "object exists" and "cleanup is
guaranteed" for an exception to land in.

## Can a third replica be added to a running Primary?

**Not today, without a code change.** `NodeConfig.ReplicaTargets` is read once, in the
constructor, into `_replicaTargets` - a plain `IReadOnlyList<ReplicaTarget>` with no
mutator. `StartAsync` launches exactly one `ReplicaLinkLoopAsync` per target that
existed at that moment:

```csharp
foreach (var target in _replicaTargets)
    _backgroundTasks.Add(Task.Run(() => ReplicaLinkLoopAsync(target, ct), ct));
```

There's no method that adds to this list or starts an additional link loop after
`StartAsync` has already run.

**What's already handled vs. what's missing.** On the new replica's own side, nothing
would need to change - the `HELLO`/catch-up mechanism already treats "a replica
connects for the first time with `AppliedSeq: 0`" and "a replica reconnects after a
drop" as the same code path (see the HELLO/REPLICATE section above), so a genuinely
new replica joining mid-flight is already correctness-handled by the existing
convergence logic. The only missing piece is entirely on the Primary side: something
like an `AddReplicaAsync(ReplicaTarget)` method that appends to the target list and
starts one more `ReplicaLinkLoopAsync` for it, callable while the Primary is already
running.

## Why `ApplyReplicatedEntry` treats "not Set" as "Del", and advances `Seq` either way

```csharp
if (msg.Op == WireOp.Set)
    _store.Set(msg.Key!, msg.Value);
else
    _store.Delete(msg.Key!);

_appliedSeq.Advance(seq);
```

**Why `else` instead of an explicit `== WireOp.Del` check:** `WireOp` only ever has two
values, `Set` and `Del`. Every `REPLICATE` message a Replica receives was built by
`ReplicaLinkLoopAsync` directly from a `ReplicationEntry`, and every `ReplicationEntry`
was created by `_log.Append(...)`, called from exactly two places -
`HandleSet` (`WireOp.Set`) and `HandleDelete` (`WireOp.Del`). There's no third value
that can reach this method, so `else` is exhaustive in practice, not a shortcut taken
at the cost of a missing case.

**Why `Seq` advances on a delete too, not just a set:** `_appliedSeq` is exactly what a
Replica's read-your-writes wait checks (`HandleGetAsync`'s
`_appliedSeq.WaitForAsync(minSeq, ...)`). If a delete didn't advance it, a client that
did `SET key` then `DEL key` then `GET key` with `minSeq` set to the delete's own `Seq`
on a replica would wait out the full timeout - the delete would already be correctly
applied to `_store`, but `minSeq` would never be considered "reached." Advancing
`_appliedSeq` for every applied entry, not just sets, is what makes `minSeq` mean "this
replica has processed everything up to and including operation N," rather than "up to
and including the Nth *set*."

## Logging: `ILogger<Node>`, with `AppliedSeq` (not wall-clock time) for cross-node order

**Motivation.** Before this, every error path in `Node.cs` was either silent
(`catch (IOException) { /* peer gone */ }`) or, worse, actively swallowed an exception
type nobody would think to look for. The JsonException fix above is the concrete
example: it made a malformed frame get *caught* correctly, but nothing about that fix
made it *visible* - on three separate real machines, a node could drop connections
indefinitely and there would be nothing anywhere to look at.

**Decision:** `Node` takes an optional `ILogger<Node>? logger = null` in its
constructor, defaulting to `NullLogger<Node>.Instance` when not supplied - so every
existing construction site (`TestHarness`, the old `Program.cs`) keeps compiling and
running exactly as before, silently, unless a real logger is explicitly wired in.
`DistributedCache.Core` only takes a dependency on
`Microsoft.Extensions.Logging.Abstractions` (interfaces + `NullLogger`, no concrete
provider) - `DistributedCache.App` is the one that references
`Microsoft.Extensions.Logging.Console` and actually constructs a console logger, since
deciding *where* logs go is a composition-root concern, not a library concern.

**Every line carries three things: the node's name, a UTC timestamp, and its own
`AppliedSeq` - each answering a different question.** `Name` says whose line it is,
once three nodes' logs are interleaved. The timestamp (`TimestampFormat` +
`UseUtcTimestamp = true` in the demo's console formatter) answers "roughly when," for
a human eyeballing a rough window - "something went wrong around 14:40." `AppliedSeq`
answers something a clock can't: on three *real* machines, wall-clock time is exactly
what this project already argued can't be trusted for ordering between nodes - the
whole reason `Seq` exists instead of a timestamp in the wire protocol (see the
Lamport-clock discussion this grew out of). `_appliedSeq.Value` is the one value this
system already computes that's guaranteed monotonic per node regardless of any clock
(`SeqGate.Advance` only moves forward). Two lines from *different* nodes can be
compared by `AppliedSeq` and the comparison means something real - "A reached
AppliedSeq=1 before B or C did" is a fact, not an inference from possibly-skewed
clocks. In practice: the timestamp finds the neighborhood of an incident; `AppliedSeq`
settles who-did-what-first once you're standing in it.

This ambient `[AppliedSeq=N]` (the node's state *when the line was written*) is a
different thing from the **event-specific** `Seq`/`MinSeq` some lines also carry (the
Seq *the line is actually about* - a write, a wait target, a dedup check). For the
node that just performed a write, the two coincide on the same line - not a bug,
just `_appliedSeq.Advance(seq)` running in the same locked block as the write itself.

**What's logged, and at what level:**

| Where | Level | What |
|---|---|---|
| `StartAsync`/`StopAsync` | Information | role, ports, stop |
| `HandleClientAsync` read - malformed frame | Warning | client's remote endpoint + exception |
| `HandleClientAsync` read - clean disconnect | Debug | routine, not an error |
| `HandleSet`/`HandleDelete` - `NOT_PRIMARY` | Warning | a client wrote to the wrong node |
| `HandleSet`/`HandleDelete` - success | Information | key + the write's own Seq |
| `HandleGetAsync` - `STALE_TIMEOUT` | Warning | a replica didn't catch up to `minSeq` in time |
| `HandleReplicationConnectionAsync` - accept, HELLO sent | Information | |
| `HandleReplicationConnectionAsync` - malformed frame / lost primary | Warning | |
| `ApplyReplicatedEntry` - dedup skip (`seq <= AppliedSeq`) | Warning | see below |
| `ApplyReplicatedEntry` - success | Information | op, key, Seq |
| `ReplicaLinkLoopAsync` - connected, resuming from seq N | Information | |
| `ReplicaLinkLoopAsync` - lost replica, retrying | Warning | includes the exception |

**Why the dedup-skip in `ApplyReplicatedEntry` is Warning, not Debug:** this is the
exact code path where the documented Primary-restart Seq-collision bug (above) would
show up in practice - a replica silently discarding writes it actually needs, while
believing they're duplicates. Under normal operation (a genuine resend after a replica
reconnect) this line firing once or twice is expected and harmless. But if it starts
firing *continuously* after a Primary restart, that's the bug from this same file
manifesting live - Warning, not Debug, so it isn't lost in routine noise, and the log
message says so directly rather than making the reader re-derive it.

**Demo wiring**, in `DistributedCache.App/Program.cs`:

```csharp
using var loggerFactory = LoggerFactory.Create(builder => builder
    .AddSimpleConsole(o =>
    {
        o.SingleLine = true;
        o.UseUtcTimestamp = true;
        o.TimestampFormat = "HH:mm:ss.fff ";
    })
    .SetMinimumLevel(LogLevel.Information));

var replicaB = new Node(new NodeConfig { ... }, loggerFactory.CreateLogger<Node>());
```

A real captured run - note the two new `applied SET`/`applied replicated SET` lines,
and `AppliedSeq` reaching 1 on `A` (the Primary's own write) before it does on `B`/`C`
(the replicated copies), which is a causal fact regardless of how close together the
timestamps land:

```
14:45:15.056 info: DistributedCache.Core.Node[0] B [AppliedSeq=0]: started as Replica, listening for clients on port 6001
14:45:15.070 info: DistributedCache.Core.Node[0] B [AppliedSeq=0]: listening for replication connections on port 6101
14:45:15.071 info: DistributedCache.Core.Node[0] C [AppliedSeq=0]: started as Replica, listening for clients on port 6002
14:45:15.071 info: DistributedCache.Core.Node[0] C [AppliedSeq=0]: listening for replication connections on port 6102
14:45:15.071 info: DistributedCache.Core.Node[0] A [AppliedSeq=0]: started as Primary, listening for clients on port 6000
14:45:15.078 info: DistributedCache.Core.Node[0] C [AppliedSeq=0]: accepted replication connection from 127.0.0.1:54583
14:45:15.078 info: DistributedCache.Core.Node[0] B [AppliedSeq=0]: accepted replication connection from 127.0.0.1:54582
14:45:15.098 info: DistributedCache.Core.Node[0] B [AppliedSeq=0]: sent HELLO reporting AppliedSeq=0
14:45:15.098 info: DistributedCache.Core.Node[0] C [AppliedSeq=0]: sent HELLO reporting AppliedSeq=0
14:45:15.101 info: DistributedCache.Core.Node[0] A [AppliedSeq=0]: connected to replica ReplicaTarget { Host = 127.0.0.1, Port = 6101 }, resuming replication from seq 0
14:45:15.101 info: DistributedCache.Core.Node[0] A [AppliedSeq=0]: connected to replica ReplicaTarget { Host = 127.0.0.1, Port = 6102 }, resuming replication from seq 0
14:45:15.376 info: DistributedCache.Core.Node[0] A [AppliedSeq=1]: applied SET key=users/1 seq=1
14:45:15.402 info: DistributedCache.Core.Node[0] B [AppliedSeq=1]: applied replicated SET key=users/1 seq=1
14:45:15.402 info: DistributedCache.Core.Node[0] C [AppliedSeq=1]: applied replicated SET key=users/1 seq=1
14:45:15.436 info: DistributedCache.Core.Node[0] A [AppliedSeq=1]: stopped
14:45:15.436 info: DistributedCache.Core.Node[0] B [AppliedSeq=1]: stopped
14:45:15.436 info: DistributedCache.Core.Node[0] C [AppliedSeq=1]: stopped
```
