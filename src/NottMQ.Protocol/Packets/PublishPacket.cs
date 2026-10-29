using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT PUBLISH packet (OASIS MQTT v3.1.1, section 3.3).
/// </summary>
public record PublishPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.Publish;

    public string Topic { get; init; } = string.Empty;
    public ushort? PacketId { get; init; }
    public MqttQoS QualityOfService { get; init; } = MqttQoS.AtMostOnce;
    public bool Duplicate { get; init; }
    public bool Retain { get; init; }
    public ReadOnlyMemory<byte> Payload { get; init; } = ReadOnlyMemory<byte>.Empty;

    public virtual bool Equals(PublishPacket? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        return Topic == other.Topic &&
               PacketId == other.PacketId &&
               QualityOfService == other.QualityOfService &&
               Duplicate == other.Duplicate &&
               Retain == other.Retain &&
               Payload.Span.SequenceEqual(other.Payload.Span);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Topic);
        hash.Add(PacketId);
        hash.Add((int)QualityOfService);
        hash.Add(Duplicate);
        hash.Add(Retain);
        hash.Add(Payload.Length);
        if (Payload.Length > 0)
        {
            hash.Add(Payload.Span[0]);
        }
        return hash.ToHashCode();
    }
}
