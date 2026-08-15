namespace DistributedCache.Core;

public sealed record ReplicationEntry(long Seq, string Op, string Key, string? Value);

/// <summary>
/// Primary-only: the full in-order history of applied writes, kept purely in memory.
/// This is what makes "reconnect and resend, including already-applied messages" possible
/// without disk persistence (which is out of scope) - the tradeoff is that this list grows
/// for the lifetime of the process (see KeyDecisions.md, "what was left out").
/// </summary>
public sealed class ReplicationLog
{
    private readonly List<ReplicationEntry> _entries = new();
    private readonly object _lock = new();
    private readonly SeqGate _appendedGate = new();

    public long LastSeq => _appendedGate.Value;

    public long Append(string op, string key, string? value)
    {
        lock (_lock)
        {
            long seq = _entries.Count + 1;
            _entries.Add(new ReplicationEntry(seq, op, key, value));
            _appendedGate.Advance(seq);
            return seq;
        }
    }

    /// <summary>All entries with Seq strictly greater than <paramref name="afterSeq"/>, in order.</summary>
    public IReadOnlyList<ReplicationEntry> From(long afterSeq)
    {
        lock (_lock)
        {
            if (afterSeq >= _entries.Count) return Array.Empty<ReplicationEntry>();
            int skip = (int)Math.Max(0, afterSeq);
            return _entries.GetRange(skip, _entries.Count - skip);
        }
    }

    /// <summary>Waits until an entry past <paramref name="afterSeq"/> has been appended, or timeout.</summary>
    public Task<bool> WaitForMoreAsync(long afterSeq, TimeSpan timeout, CancellationToken ct) =>
        _appendedGate.WaitForAsync(afterSeq + 1, timeout, ct);
}
