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
