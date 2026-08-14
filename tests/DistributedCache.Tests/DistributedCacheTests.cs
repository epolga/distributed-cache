using DistributedCache.Core;
using Xunit;

namespace DistributedCache.Tests;

public class DistributedCacheTests
{
    [Fact]
    public async Task HappyPath_SetThenGetOnPrimary_ReturnsWrittenValue()
    {
        var primary = TestHarness.CreatePrimary("A", out _);
        await primary.StartAsync();
        await using var _dispose = primary;
        await using var client = TestHarness.ClientFor(primary);

        await client.SetAsync("users/1", "Olga");
        var (found, value) = await client.GetAsync("users/1");

        Assert.True(found);
        Assert.Equal("Olga", value);
    }

    [Fact]
    public async Task Set_SentToReplica_IsRejectedWithNotPrimary()
    {
        var replica = TestHarness.CreateReplica("B", out _, out _);
        await replica.StartAsync();
        await using var _dispose = replica;
        await using var client = TestHarness.ClientFor(replica);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => client.SetAsync("users/1", "Olga"));
        Assert.Contains("not the primary", ex.Message);
    }

    [Fact]
    public async Task ReadYourWrites_GetOnReplicaWithMinSeq_NeverObservesStaleValue()
    {
        var replicaB = TestHarness.CreateReplica("B", out _, out int replPortB);
        var primary = TestHarness.CreatePrimary("A", out _, new ReplicaTarget("127.0.0.1", replPortB));
        await replicaB.StartAsync();
        await primary.StartAsync();
        await using var d1 = replicaB;
        await using var d2 = primary;
        await Task.Delay(200); // let the replica link establish once, before the loop below

        await using var writer = TestHarness.ClientFor(primary);
        await using var reader = TestHarness.ClientFor(replicaB);

        // No sleeps between write and read: without the MinSeq wait this would be racy and
        // fail intermittently. Looping makes a missing guarantee show up reliably.
        for (int i = 0; i < 50; i++)
        {
            string key = $"k{i}";
            string val = $"v{i}";
            long seq = await writer.SetAsync(key, val);
            var (found, value) = await reader.GetAsync(key, minSeq: seq);
            Assert.True(found, $"key {key} not found on replica immediately after a read-your-writes GET");
            Assert.Equal(val, value);
        }
    }

    [Fact]
    public async Task ConcurrentWrites_ToPrimary_ConvergeOnAllReplicas()
    {
        var replicaB = TestHarness.CreateReplica("B", out _, out int replPortB);
        var replicaC = TestHarness.CreateReplica("C", out _, out int replPortC);
        var primary = TestHarness.CreatePrimary("A", out _,
            new ReplicaTarget("127.0.0.1", replPortB), new ReplicaTarget("127.0.0.1", replPortC));
        await replicaB.StartAsync();
        await replicaC.StartAsync();
        await primary.StartAsync();
        await using var d1 = replicaB;
        await using var d2 = replicaC;
        await using var d3 = primary;
        await Task.Delay(200);

        const int writerCount = 10;
        const int writesPerWriter = 20;
        var writeTasks = Enumerable.Range(0, writerCount).Select(async w =>
        {
            await using var client = TestHarness.ClientFor(primary);
            for (int i = 0; i < writesPerWriter; i++)
                await client.SetAsync($"w{w}/k{i}", $"v{w}-{i}");
        });
        await Task.WhenAll(writeTasks);

        long finalSeq = primary.AppliedSeq;
        Assert.Equal(writerCount * writesPerWriter, finalSeq);

        // Convergence is eventual, not synchronous with the write - poll with a bounded timeout
        // rather than asserting immediately.
        await using var probe = TestHarness.ClientFor(replicaB);
        var (found, _) = await probe.GetAsync($"w{writerCount - 1}/k{writesPerWriter - 1}", minSeq: finalSeq);
        Assert.True(found);

        await using var probeC = TestHarness.ClientFor(replicaC);
        var (foundC, _) = await probeC.GetAsync($"w{writerCount - 1}/k{writesPerWriter - 1}", minSeq: finalSeq);
        Assert.True(foundC);

        Assert.Equal(writerCount * writesPerWriter, replicaB.AppliedSeq);
        Assert.Equal(writerCount * writesPerWriter, replicaC.AppliedSeq);
    }

    [Fact]
    public async Task LateJoiningReplica_CatchesUpOnHistoryWrittenBeforeItConnected()
    {
        var lateReplica = TestHarness.CreateReplica("B", out _, out int replPort);
        var primary = TestHarness.CreatePrimary("A", out _, new ReplicaTarget("127.0.0.1", replPort));

        await primary.StartAsync();
        await using var d1 = primary;
        await using var writer = TestHarness.ClientFor(primary);

        for (int i = 0; i < 25; i++)
            await writer.SetAsync($"k{i}", $"v{i}");
        long finalSeq = primary.AppliedSeq;

        await lateReplica.StartAsync(); // now the primary's retry loop can finally connect
        await using var d2 = lateReplica;

        await using var reader = TestHarness.ClientFor(lateReplica);
        var (found, value) = await reader.GetAsync("k24", minSeq: finalSeq);

        Assert.True(found);
        Assert.Equal("v24", value);
        Assert.Equal(finalSeq, lateReplica.AppliedSeq);
    }
}
