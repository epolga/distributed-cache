using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DistributedCache.Core;

public enum NodeRole { Primary, Replica }

/// <summary>Where a primary connects to reach one replica's replication listener.</summary>
public sealed record ReplicaTarget(string Host, int Port);

public sealed class NodeConfig
{
    public required string Name { get; init; }
    public required NodeRole Role { get; init; }
    public required int ClientPort { get; init; }

    /// <summary>Replica only: port this node listens on for the primary's replication connection.</summary>
    public int? ReplicationPort { get; init; }

    /// <summary>Primary only: the replicas it must push writes to.</summary>
    public IReadOnlyList<ReplicaTarget>? ReplicaTargets { get; init; }
}

/// <summary>
/// One cluster node. Role is fixed at construction (no promotion/election - out of scope).
/// Always serves client GET/SET/DEL on ClientPort. A Primary also runs one background
/// "replica link" per configured replica; a Replica also accepts a replication connection
/// from the primary.
/// </summary>
public sealed class Node : IAsyncDisposable
{
    private const int MaxConcurrentClientConnections = 50;
    private static readonly TimeSpan ReadYourWritesTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan ReplicaReconnectDelay = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan ReplicaIdlePollTimeout = TimeSpan.FromSeconds(1);

    public string Name { get; }
    public NodeRole Role { get; }
    public int ClientPort { get; }
    public int? ReplicationPort { get; }

    private readonly InMemoryStore _store = new();
    private readonly SeqGate _appliedSeq = new();
    private readonly object _applyLock = new();
    private readonly ILogger<Node> _logger;

    // Primary-only
    private readonly ReplicationLog? _log;
    private readonly IReadOnlyList<ReplicaTarget> _replicaTargets;

    private readonly TcpListener _clientListener;
    private readonly TcpListener? _replicationListener; // replica-only
    private readonly SemaphoreSlim _connectionLimiter = new(MaxConcurrentClientConnections);

    private CancellationTokenSource? _cts;
    private readonly List<Task> _backgroundTasks = new();

    public Node(NodeConfig config, ILogger<Node>? logger = null)
    {
        _logger = logger ?? NullLogger<Node>.Instance;
        Name = config.Name;
        Role = config.Role;
        ClientPort = config.ClientPort;
        ReplicationPort = config.ReplicationPort;
        _replicaTargets = config.ReplicaTargets ?? Array.Empty<ReplicaTarget>();

        _clientListener = new TcpListener(IPAddress.Loopback, ClientPort);

        if (Role == NodeRole.Primary)
        {
            _log = new ReplicationLog();
        }
        else
        {
            if (ReplicationPort is null)
                throw new ArgumentException("Replica nodes must have a ReplicationPort.", nameof(config));
            _replicationListener = new TcpListener(IPAddress.Loopback, ReplicationPort.Value);
        }
    }

    /// <summary>Diagnostics/tests only: how far this node's local store has caught up.</summary>
    public long AppliedSeq => _appliedSeq.Value;

    public Task StartAsync()
    {
        if (_cts is not null) throw new InvalidOperationException("Already started.");
        _cts = new CancellationTokenSource();
        var ct = _cts.Token;

        _clientListener.Start();
        _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: started as {Role}, listening for clients on port {ClientPort}", Name, _appliedSeq.Value, Role, ClientPort);
        _backgroundTasks.Add(Task.Run(() => AcceptClientsLoopAsync(ct), ct));

        if (Role == NodeRole.Replica)
        {
            _replicationListener!.Start();
            _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: listening for replication connections on port {ReplicationPort}", Name, _appliedSeq.Value, ReplicationPort);
            _backgroundTasks.Add(Task.Run(() => AcceptReplicationLoopAsync(ct), ct));
        }
        else
        {
            foreach (var target in _replicaTargets)
                _backgroundTasks.Add(Task.Run(() => ReplicaLinkLoopAsync(target, ct), ct));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        _cts.Cancel();
        _clientListener.Stop();
        _replicationListener?.Stop();

        try
        {
            await Task.WhenAll(_backgroundTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* expected on shutdown */ }
        catch (SocketException) { /* listener stopped mid-accept - expected on shutdown */ }

        _cts.Dispose();
        _cts = null;
        _backgroundTasks.Clear();
        _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: stopped", Name, _appliedSeq.Value);
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    // ---------------------------------------------------------------- client-facing side

    private async Task AcceptClientsLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _clientListener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                return;
            }

            _ = HandleClientAsync(client, ct); // fire-and-forget; each connection is independent
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        // Bounds the number of connections actively being served - a slow/malicious burst of
        // connects queues here instead of spawning unbounded reader tasks (see NOTES.md).
        await _connectionLimiter.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            using (client)
            {
                client.NoDelay = true;
                var stream = client.GetStream();
                while (!ct.IsCancellationRequested)
                {
                    WireMessage? request;
                    try
                    {
                        request = await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false);
                    }
                    catch (InvalidDataException ex)
                    {
                        _logger.LogWarning(ex, "{Node} [AppliedSeq={AppliedSeq}]: malformed frame from client {Endpoint}, dropping connection", Name, _appliedSeq.Value, client.Client.RemoteEndPoint);
                        return;
                    }
                    catch (IOException)
                    {
                        _logger.LogDebug("{Node} [AppliedSeq={AppliedSeq}]: client {Endpoint} disconnected", Name, _appliedSeq.Value, client.Client.RemoteEndPoint);
                        return; // peer reset/closed abruptly
                    }
                    if (request is null) return; // clean disconnect

                    WireMessage response = request.Kind switch
                    {
                        WireKind.Get => await HandleGetAsync(request, ct).ConfigureAwait(false),
                        WireKind.Set => HandleSet(request),
                        WireKind.Del => HandleDelete(request),
                        _ => new WireMessage { Kind = WireKind.Response, Status = WireStatus.Error }
                    };

                    await WireCodec.WriteMessageAsync(stream, response, ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { /* shutting down */ }
        catch (IOException) { /* peer gone */ }
        finally
        {
            _connectionLimiter.Release();
        }
    }

    private async Task<WireMessage> HandleGetAsync(WireMessage request, CancellationToken ct)
    {
        if (request.MinSeq is long minSeq && minSeq > 0)
        {
            bool caughtUp = await _appliedSeq.WaitForAsync(minSeq, ReadYourWritesTimeout, ct).ConfigureAwait(false);
            if (!caughtUp)
            {
                _logger.LogWarning("{Node} [AppliedSeq={AppliedSeq}]: read-your-writes wait for seq {MinSeq} timed out after {Timeout}", Name, _appliedSeq.Value, minSeq, ReadYourWritesTimeout);
                return new WireMessage { Kind = WireKind.Response, Status = WireStatus.StaleTimeout };
            }
        }

        if (_store.TryGet(request.Key!, out var value))
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.Ok, Value = value };

        return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotFound };
    }

    private WireMessage HandleSet(WireMessage request)
    {
        if (Role != NodeRole.Primary)
        {
            _logger.LogWarning("{Node} [AppliedSeq={AppliedSeq}]: rejected SET for key {Key} - not primary", Name, _appliedSeq.Value, request.Key);
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotPrimary };
        }

        long seq;
        lock (_applyLock)
        {
            seq = _log!.Append(WireOp.Set, request.Key!, request.Value);
            _store.Set(request.Key!, request.Value);
            _appliedSeq.Advance(seq);
        }
        _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: applied SET key={Key} seq={Seq}", Name, _appliedSeq.Value, request.Key, seq);
        return new WireMessage { Kind = WireKind.Response, Status = WireStatus.Ok, Seq = seq };
    }

    private WireMessage HandleDelete(WireMessage request)
    {
        if (Role != NodeRole.Primary)
        {
            _logger.LogWarning("{Node} [AppliedSeq={AppliedSeq}]: rejected DEL for key {Key} - not primary", Name, _appliedSeq.Value, request.Key);
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotPrimary };
        }

        long seq;
        lock (_applyLock)
        {
            seq = _log!.Append(WireOp.Del, request.Key!, null);
            _store.Delete(request.Key!);
            _appliedSeq.Advance(seq);
        }
        _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: applied DEL key={Key} seq={Seq}", Name, _appliedSeq.Value, request.Key, seq);
        return new WireMessage { Kind = WireKind.Response, Status = WireStatus.Ok, Seq = seq };
    }

    // ---------------------------------------------------------------- replica-side (inbound replication)

    private async Task AcceptReplicationLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TcpClient conn;
            try
            {
                conn = await _replicationListener!.AcceptTcpClientAsync(ct).ConfigureAwait(false);
            }
            catch (Exception) when (ct.IsCancellationRequested)
            {
                return;
            }

            _ = HandleReplicationConnectionAsync(conn, ct); // primary reconnecting replaces the old handler
        }
    }

    private async Task HandleReplicationConnectionAsync(TcpClient conn, CancellationToken ct)
    {
        try
        {
            using (conn)
            {
                conn.NoDelay = true;
                var stream = conn.GetStream();
                _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: accepted replication connection from {Endpoint}", Name, _appliedSeq.Value, conn.Client.RemoteEndPoint);

                // Tell the primary where we left off, so it knows what to (re)send. This is the
                // one mechanism that makes "primary resends on reconnect" and "late-joining
                // replica catches up" the same code path.
                await WireCodec.WriteMessageAsync(
                    stream,
                    new WireMessage { Kind = WireKind.Hello, Seq = _appliedSeq.Value },
                    ct).ConfigureAwait(false);
                _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: sent HELLO reporting AppliedSeq={AppliedSeq}", Name, _appliedSeq.Value, _appliedSeq.Value);

                while (!ct.IsCancellationRequested)
                {
                    WireMessage? msg;
                    try
                    {
                        msg = await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false);
                    }
                    catch (InvalidDataException ex)
                    {
                        _logger.LogWarning(ex, "{Node} [AppliedSeq={AppliedSeq}]: malformed frame on replication connection, dropping it", Name, _appliedSeq.Value);
                        return;
                    }
                    catch (IOException)
                    {
                        _logger.LogWarning("{Node} [AppliedSeq={AppliedSeq}]: lost connection to primary, will re-HELLO on reconnect", Name, _appliedSeq.Value);
                        return; // primary dropped - it will reconnect and we'll re-Hello then
                    }
                    if (msg is null) return;

                    if (msg.Kind == WireKind.Replicate)
                        ApplyReplicatedEntry(msg);
                }
            }
        }
        catch (OperationCanceledException) { /* shutting down */ }
        catch (IOException) { /* peer gone */ }
    }

    private void ApplyReplicatedEntry(WireMessage msg)
    {
        lock (_applyLock)
        {
            long seq = msg.Seq!.Value;
            if (seq <= _appliedSeq.Value)
            {
                _logger.LogWarning(
                    "{Node} [AppliedSeq={AppliedSeq}]: skipped already-applied seq {Seq} - " +
                    "expected after a resend/reconnect, but also how the primary-restart seq-collision bug " +
                    "in Design.md would manifest", Name, _appliedSeq.Value, seq);
                return; // already applied - dedup after resend/reconnect (convergence requirement)
            }

            if (msg.Op == WireOp.Set)
                _store.Set(msg.Key!, msg.Value);
            else
                _store.Delete(msg.Key!);

            _appliedSeq.Advance(seq);
            _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: applied replicated {Op} key={Key} seq={Seq}", Name, _appliedSeq.Value, msg.Op, msg.Key, seq);
        }
    }

    // ---------------------------------------------------------------- primary-side (outbound replication)

    private async Task ReplicaLinkLoopAsync(ReplicaTarget target, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                await tcp.ConnectAsync(target.Host, target.Port, ct).ConfigureAwait(false);
                tcp.NoDelay = true;
                var stream = tcp.GetStream();

                var hello = await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false)
                            ?? throw new IOException("Replica closed before Hello.");
                long lastSent = hello.Seq ?? 0;
                _logger.LogInformation("{Node} [AppliedSeq={AppliedSeq}]: connected to replica {Target}, resuming replication from seq {LastSent}", Name, _appliedSeq.Value, target, lastSent);

                while (!ct.IsCancellationRequested)
                {
                    foreach (var entry in _log!.From(lastSent))
                    {
                        await WireCodec.WriteMessageAsync(stream, new WireMessage
                        {
                            Kind = WireKind.Replicate,
                            Op = entry.Op,
                            Key = entry.Key,
                            Value = entry.Value,
                            Seq = entry.Seq
                        }, ct).ConfigureAwait(false);
                        lastSent = entry.Seq;
                    }

                    // Nothing new right now - wait to be woken by the next Append instead of
                    // spinning. This wait (not a buffered queue) is where replication
                    // backpressure lives: a slow replica just leaves entries unsent in the
                    // log rather than piling them into a growing in-memory queue.
                    await _log.WaitForMoreAsync(lastSent, ReplicaIdlePollTimeout, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                return; // shutting down
            }
            catch (Exception ex)
            {
                // Connection refused / reset / EOF - replica is down or restarting.
                // Back off briefly and retry; the next Hello tells us where to resume.
                _logger.LogWarning(ex, "{Node} [AppliedSeq={AppliedSeq}]: lost connection to replica {Target}, retrying in {Delay}", Name, _appliedSeq.Value, target, ReplicaReconnectDelay);
                try { await Task.Delay(ReplicaReconnectDelay, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }
}
