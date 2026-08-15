using System.Buffers.Binary;
using System.Text.Json;

namespace DistributedCache.Core;

/// <summary>
/// Single flat message used on both the client-&gt;node wire and the primary&lt;-&gt;replica
/// replication wire. Kind selects which fields are meaningful:
///   SET / DEL / GET  : client request.  Key/Value/MinSeq as applicable.
///   RESPONSE         : reply to a client request. Status/Value/Seq as applicable.
///   HELLO            : first message a replica sends on a fresh replication connection,
///                       carrying the replica's last applied Seq so the primary knows
///                       where to resume streaming from.
///   REPLICATE        : one applied write, streamed primary -> replica. Op is "SET"/"DEL".
/// </summary>
public sealed class WireMessage
{
    public string Kind { get; set; } = "";
    public string? Op { get; set; }
    public string? Key { get; set; }
    public string? Value { get; set; }
    public long? Seq { get; set; }
    public long? MinSeq { get; set; }
    public string? Status { get; set; }
}

public static class WireStatus
{
    public const string Ok = "OK";
    public const string NotFound = "NOTFOUND";
    public const string NotPrimary = "NOT_PRIMARY";
    public const string StaleTimeout = "STALE_TIMEOUT";
    public const string Error = "ERROR";
}

/// <summary>Values for <see cref="WireMessage.Kind"/>.</summary>
public static class WireKind
{
    public const string Set = "SET";
    public const string Del = "DEL";
    public const string Get = "GET";
    public const string Response = "RESPONSE";
    public const string Hello = "HELLO";
    public const string Replicate = "REPLICATE";
}

/// <summary>Values for <see cref="WireMessage.Op"/> (the replicated operation, distinct from <see cref="WireKind"/>).</summary>
public static class WireOp
{
    public const string Set = "SET";
    public const string Del = "DEL";
}

/// <summary>
/// Length-prefixed framing over a raw stream: 4-byte big-endian payload length, then the
/// payload itself. Guards against unbounded allocation from a malformed/hostile length.
/// </summary>
public static class Frame
{
    public const int MaxPayloadBytes = 1024 * 1024; // 1 MiB; generous for cache values, bounds allocation

    public static async Task WriteAsync(Stream stream, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var header = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
        await stream.WriteAsync(header, ct).ConfigureAwait(false);
        await stream.WriteAsync(payload, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Returns null on clean EOF (peer closed before/between frames).</summary>
    public static async Task<byte[]?> ReadAsync(Stream stream, CancellationToken ct)
    {
        var header = new byte[4];
        if (!await ReadExactAsync(stream, header, ct).ConfigureAwait(false))
            return null;

        int length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length < 0 || length > MaxPayloadBytes)
            throw new InvalidDataException($"Frame length {length} out of bounds (max {MaxPayloadBytes}).");

        var payload = new byte[length];
        if (!await ReadExactAsync(stream, payload, ct).ConfigureAwait(false))
            throw new EndOfStreamException("Connection closed mid-frame.");

        return payload;
    }

    private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset), ct).ConfigureAwait(false);
            if (read == 0)
                return offset == 0 ? false : throw new EndOfStreamException("Connection closed mid-frame.");
            offset += read;
        }
        return true;
    }
}

public static class WireCodec
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static async Task WriteMessageAsync(Stream stream, WireMessage message, CancellationToken ct)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(message, Options);
        await Frame.WriteAsync(stream, bytes, ct).ConfigureAwait(false);
    }

    public static async Task<WireMessage?> ReadMessageAsync(Stream stream, CancellationToken ct)
    {
        byte[]? bytes = await Frame.ReadAsync(stream, ct).ConfigureAwait(false);
        if (bytes is null) return null;
        try
        {
            return JsonSerializer.Deserialize<WireMessage>(bytes, Options)
                   ?? throw new InvalidDataException("Empty/invalid wire message.");
        }
        catch (JsonException ex)
        {
            // InvalidDataException : IOException, so this is caught by every existing
            // catch (IOException) call site without touching Node.cs - see DESIGN.md and KeyDecisions.md.
            throw new InvalidDataException("Malformed wire message.", ex);
        }
    }
}
