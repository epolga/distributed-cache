using System.Net;
using System.Net.Sockets;
using DistributedCache.Core;

namespace DistributedCache.Tests;

/// <summary>Test-only helpers: free-port allocation and node construction shortcuts.</summary>
internal static class TestHarness
{
    public static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public static Node CreateReplica(string name, out int clientPort, out int replicationPort)
    {
        clientPort = GetFreePort();
        replicationPort = GetFreePort();
        return new Node(new NodeConfig
        {
            Name = name,
            Role = NodeRole.Replica,
            ClientPort = clientPort,
            ReplicationPort = replicationPort
        });
    }

    public static Node CreatePrimary(string name, out int clientPort, params ReplicaTarget[] replicas)
    {
        clientPort = GetFreePort();
        return new Node(new NodeConfig
        {
            Name = name,
            Role = NodeRole.Primary,
            ClientPort = clientPort,
            ReplicaTargets = replicas
        });
    }

    public static CacheClient ClientFor(Node node) => new("127.0.0.1", node.ClientPort);
}
