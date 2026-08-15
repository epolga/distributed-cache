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
