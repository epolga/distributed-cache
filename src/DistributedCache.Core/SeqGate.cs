namespace DistributedCache.Core;

/// <summary>
/// Tracks a monotonically increasing "applied sequence number" and lets callers await
/// until it reaches at least some value. Used for:
///   - a node's local AppliedSeq (drives the read-your-writes wait on GET), and
///   - the primary's replication log's LastAppendedSeq (wakes a stalled replica-sender
///     instead of having it poll forever with no bound).
///
/// A monotonic value under a lock, checked on a short poll interval by anyone waiting.
/// Correctness does not depend on the poll interval: WaitForAsync always re-reads the
/// live Value, so a "stale" read between polls can never happen - see NOTES.md.
/// </summary>
public sealed class SeqGate
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

    private readonly object _lock = new();
    private long _value;

    public long Value
    {
        get { lock (_lock) return _value; }
    }

    public void Advance(long seq)
    {
        lock (_lock)
        {
            if (seq > _value) _value = seq;
        }
    }

    /// <summary>Waits until Value >= target, or timeout elapses. Returns false on timeout.</summary>
    public async Task<bool> WaitForAsync(long target, TimeSpan timeout, CancellationToken ct)
    {
        if (Value >= target) return true;

        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) return Value >= target; // final check in case it just landed

            var delay = remaining < PollInterval ? remaining : PollInterval;
            await Task.Delay(delay, ct).ConfigureAwait(false);

            if (Value >= target) return true;
        }
    }
}
