using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DistributedCache.Core;

/// <summary>
/// Minimal client used directly by tests (no CLI/REPL - out of scope per the assignment).
/// One instance talks to exactly one node over a single persistent TCP connection.
/// </summary>
public sealed class CacheClient : IAsyncDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _target;
    private readonly ILogger<CacheClient> _logger;
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private readonly SemaphoreSlim _sendLock = new(1, 1); // one request in flight per connection

    public CacheClient(string host, int port, ILogger<CacheClient>? logger = null)
    {
        _host = host;
        _port = port;
        _target = $"{host}:{port}";
        _logger = logger ?? NullLogger<CacheClient>.Instance;
    }

    private async Task<NetworkStream> EnsureConnectedAsync(CancellationToken ct)
    {
        if (_stream is not null) return _stream;
        _tcp = new TcpClient { NoDelay = true };
        await _tcp.ConnectAsync(_host, _port, ct).ConfigureAwait(false);
        _stream = _tcp.GetStream();
        _logger.LogInformation("{Target}: connected", _target);
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
        switch (response.Status)
        {
            case WireStatus.Ok:
                _logger.LogInformation("{Target}: {Kind} key={Key} seq={Seq}", _target, request.Kind, request.Key, response.Seq);
                return response.Seq!.Value;
            case WireStatus.NotPrimary:
                _logger.LogWarning("{Target}: {Kind} key={Key} rejected - not primary", _target, request.Kind, request.Key);
                throw new InvalidOperationException(
                    $"Node at {_host}:{_port} is not the primary; writes must go to the primary.");
            default:
                _logger.LogWarning("{Target}: {Kind} key={Key} failed - status={Status}", _target, request.Kind, request.Key, response.Status);
                throw new InvalidOperationException($"Write failed: {response.Status}");
        }
    }

    /// <param name="minSeq">
    /// If set, the node will wait (bounded) until its local applied sequence reaches this
    /// value before answering - this is the read-your-writes knob. Pass the Seq returned by
    /// a prior SetAsync/DeleteAsync when reading the same key from a different node.
    /// </param>
    public async Task<(bool Found, string? Value)> GetAsync(string key, long? minSeq = null, CancellationToken ct = default)
    {
        var response = await SendAsync(new WireMessage { Kind = WireKind.Get, Key = key, MinSeq = minSeq }, ct).ConfigureAwait(false);
        switch (response.Status)
        {
            case WireStatus.Ok:
                return (true, response.Value);
            case WireStatus.NotFound:
                return (false, null);
            case WireStatus.StaleTimeout:
                _logger.LogWarning("{Target}: GET key={Key} timed out waiting for seq {MinSeq}", _target, key, minSeq);
                throw new TimeoutException(
                    $"Node at {_host}:{_port} did not catch up to seq {minSeq} in time.");
            default:
                _logger.LogWarning("{Target}: GET key={Key} failed - status={Status}", _target, key, response.Status);
                throw new InvalidOperationException($"Get failed: {response.Status}");
        }
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
        catch (Exception ex) when (ex is IOException or SocketException)
        {
            // SocketException: TcpClient.ConnectAsync failing (refused/unreachable) doesn't
            // raise IOException, unlike NetworkStream read/write failures - both are logged
            // here, then rethrown so the immediate caller still sees the exception too.
            _logger.LogWarning(ex, "{Target}: {Kind} key={Key} failed - connection error", _target, request.Kind, request.Key);
            throw;
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_stream is not null)
            _logger.LogInformation("{Target}: disconnected", _target);
        _stream?.Dispose();
        _tcp?.Dispose();
        return ValueTask.CompletedTask;
    }
}
