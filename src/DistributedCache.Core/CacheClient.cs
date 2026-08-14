using System.Net.Sockets;

namespace DistributedCache.Core;

/// <summary>
/// Minimal client used directly by tests (no CLI/REPL - out of scope per the assignment).
/// One instance talks to exactly one node over a single persistent TCP connection.
/// </summary>
public sealed class CacheClient : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private readonly SemaphoreSlim _sendLock = new(1, 1); // one request in flight per connection

    public CacheClient(string host, int port)
    {
        _host = host;
        _port = port;
    }

    private async Task<NetworkStream> EnsureConnectedAsync(CancellationToken ct)
    {
        if (_stream is not null) return _stream;
        _tcp = new TcpClient { NoDelay = true };
        await _tcp.ConnectAsync(_host, _port, ct).ConfigureAwait(false);
        _stream = _tcp.GetStream();
        return _stream;
    }

    /// <summary>Sets a key on the primary. Returns the write's sequence number (pass to GetAsync's minSeq for read-your-writes).</summary>
    public Task<long> SetAsync(string key, string value, CancellationToken ct = default) =>
        SendWriteAsync(new WireMessage { Kind = WireKind.Set, Key = key, Value = value }, ct);

    public Task<long> DeleteAsync(string key, CancellationToken ct = default) =>
        SendWriteAsync(new WireMessage { Kind = WireKind.Del, Key = key }, ct);

    private async Task<long> SendWriteAsync(WireMessage request, CancellationToken ct)
    {
        var response = await SendAsync(request, ct).ConfigureAwait(false);
        return response.Status switch
        {
            WireStatus.Ok => response.Seq!.Value,
            WireStatus.NotPrimary => throw new InvalidOperationException(
                $"Node at {_host}:{_port} is not the primary; writes must go to the primary."),
            _ => throw new InvalidOperationException($"Write failed: {response.Status}")
        };
    }

    /// <param name="minSeq">
    /// If set, the node will wait (bounded) until its local applied sequence reaches this
    /// value before answering - this is the read-your-writes knob. Pass the Seq returned by
    /// a prior SetAsync/DeleteAsync when reading the same key from a different node.
    /// </param>
    public async Task<(bool Found, string? Value)> GetAsync(string key, long? minSeq = null, CancellationToken ct = default)
    {
        var response = await SendAsync(new WireMessage { Kind = WireKind.Get, Key = key, MinSeq = minSeq }, ct).ConfigureAwait(false);
        return response.Status switch
        {
            WireStatus.Ok => (true, response.Value),
            WireStatus.NotFound => (false, null),
            WireStatus.StaleTimeout => throw new TimeoutException(
                $"Node at {_host}:{_port} did not catch up to seq {minSeq} in time."),
            _ => throw new InvalidOperationException($"Get failed: {response.Status}")
        };
    }

    private async Task<WireMessage> SendAsync(WireMessage request, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var stream = await EnsureConnectedAsync(ct).ConfigureAwait(false);
            await WireCodec.WriteMessageAsync(stream, request, ct).ConfigureAwait(false);
            return await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false)
                   ?? throw new IOException("Connection closed before a response arrived.");
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        _stream?.Dispose();
        _tcp?.Dispose();
        return ValueTask.CompletedTask;
    }
}
