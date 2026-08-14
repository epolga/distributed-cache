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
