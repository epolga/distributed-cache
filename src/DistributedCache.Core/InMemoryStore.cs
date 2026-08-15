using System.Collections.Concurrent;

namespace DistributedCache.Core;

/// <summary>Plain in-memory key/value store. No eviction, no TTL, no persistence (by design - see KeyDecisions.md).</summary>
public sealed class InMemoryStore
{
    private readonly ConcurrentDictionary<string, string?> _data = new();

    public void Set(string key, string? value) => _data[key] = value;

    public void Delete(string key) => _data.TryRemove(key, out _);

    public bool TryGet(string key, out string? value) => _data.TryGetValue(key, out value);

    public int Count => _data.Count;
}
