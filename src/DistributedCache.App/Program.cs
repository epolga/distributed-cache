using DistributedCache.Core;

// Manual smoke-test / demo: spins up a 3-node cluster on loopback in a single process
// and exercises the primary write-path + read-your-writes.

var replicaB = new Node(new NodeConfig { Name = "B", Role = NodeRole.Replica, ClientPort = 6001, ReplicationPort = 6101 });
var replicaC = new Node(new NodeConfig { Name = "C", Role = NodeRole.Replica, ClientPort = 6002, ReplicationPort = 6102 });
var primary = new Node(new NodeConfig
{
    Name = "A",
    Role = NodeRole.Primary,
    ClientPort = 6000,
    ReplicaTargets = new[] { new ReplicaTarget("127.0.0.1", 6101), new ReplicaTarget("127.0.0.1", 6102) }
});

await replicaB.StartAsync();
await replicaC.StartAsync();
await primary.StartAsync();
await Task.Delay(300); // let the replica links connect

await using var writer = new CacheClient("127.0.0.1", primary.ClientPort);
await using var readerB = new CacheClient("127.0.0.1", replicaB.ClientPort);
await using var readerC = new CacheClient("127.0.0.1", replicaC.ClientPort);

long seq = await writer.SetAsync("users/1", "Olga");
Console.WriteLine($"SET users/1 = Olga  (seq={seq})");

var fromB = await readerB.GetAsync("users/1", minSeq: seq);
var fromC = await readerC.GetAsync("users/1", minSeq: seq);
Console.WriteLine($"GET users/1 from B -> {(fromB.Found ? fromB.Value : "<missing>")}");
Console.WriteLine($"GET users/1 from C -> {(fromC.Found ? fromC.Value : "<missing>")}");

await primary.StopAsync();
await replicaB.StopAsync();
await replicaC.StopAsync();
