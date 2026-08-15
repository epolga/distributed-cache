# Distributed Cache — 3-node Primary/Replica over TCP

A fixed-topology, in-memory cache: one Primary (writes) and two Replicas (reads),
replicated over a hand-rolled length-prefixed TCP protocol. See `Design.md` for the
design decisions behind it.

## Layout

```
src/DistributedCache.Core/   protocol, store, replication log, Node, CacheClient
src/DistributedCache.App/    tiny console demo: starts A/B/C on loopback, does one SET + two GETs
tests/DistributedCache.Tests/  xUnit behavioral tests
```

## Build

Requires the .NET 8 SDK.

```
dotnet build
```

## Run the tests

```
dotnet test
```

## Run the demo

```
dotnet run --project src/DistributedCache.App
```

Starts all three nodes in-process on `127.0.0.1` (ports 6000-6002 client, 6101-6102
replication), writes one key on the Primary, and reads it back from both Replicas using
the read-your-writes `minSeq` handshake.
