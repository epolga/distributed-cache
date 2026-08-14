using System.Net;
using System.Net.Sockets;

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

    // Primary-only
    private readonly ReplicationLog? _log;
    private readonly IReadOnlyList<ReplicaTarget> _replicaTargets;

    private readonly TcpListener _clientListener;
    private readonly TcpListener? _replicationListener; // replica-only
    private readonly SemaphoreSlim _connectionLimiter = new(MaxConcurrentClientConnections);

    private CancellationTokenSource? _cts;
    private readonly List<Task> _backgroundTasks = new();

    public Node(NodeConfig config)
    {
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
        _backgroundTasks.Add(Task.Run(() => AcceptClientsLoopAsync(ct), ct));

        if (Role == NodeRole.Replica)
        {
            _replicationListener!.Start();
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
                    catch (IOException)
                    {
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
                return new WireMessage { Kind = WireKind.Response, Status = WireStatus.StaleTimeout };
        }

        if (_store.TryGet(request.Key!, out var value))
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.Ok, Value = value };

        return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotFound };
    }

    private WireMessage HandleSet(WireMessage request)
    {
        if (Role != NodeRole.Primary)
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotPrimary };

        long seq;
        lock (_applyLock)
        {
            seq = _log!.Append(WireOp.Set, request.Key!, request.Value);
            _store.Set(request.Key!, request.Value);
            _appliedSeq.Advance(seq);
        }
        return new WireMessage { Kind = WireKind.Response, Status = WireStatus.Ok, Seq = seq };
    }

    private WireMessage HandleDelete(WireMessage request)
    {
        if (Role != NodeRole.Primary)
            return new WireMessage { Kind = WireKind.Response, Status = WireStatus.NotPrimary };

        long seq;
        lock (_applyLock)
        {
            seq = _log!.Append(WireOp.Del, request.Key!, null);
            _store.Delete(request.Key!);
            _appliedSeq.Advance(seq);
        }
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

                // Tell the primary where we left off, so it knows what to (re)send. This is the
                // one mechanism that makes "primary resends on reconnect" and "late-joining
                // replica catches up" the same code path.
                await WireCodec.WriteMessageAsync(
                    stream,
                    new WireMessage { Kind = WireKind.Hello, Seq = _appliedSeq.Value },
                    ct).ConfigureAwait(false);

                while (!ct.IsCancellationRequested)
                {
                    WireMessage? msg;
                    try
                    {
                        msg = await WireCodec.ReadMessageAsync(stream, ct).ConfigureAwait(false);
                    }
                    catch (IOException)
                    {
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
                return; // already applied - dedup after resend/reconnect (convergence requirement)

            if (msg.Op == WireOp.Set)
                _store.Set(msg.Key!, msg.Value);
            else
                _store.Delete(msg.Key!);

            _appliedSeq.Advance(seq);
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
            catch (Exception)
            {
                // Connection refused / reset / EOF - replica is down or restarting.
                // Back off briefly and retry; the next Hello tells us where to resume.
                try { await Task.Delay(ReplicaReconnectDelay, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }
}
