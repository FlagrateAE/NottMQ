using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT UNSUBSCRIBE packet (OASIS MQTT v3.1.1, section 3.10).
/// </summary>
public record UnsubscribePacket : MqttPacket
{
    public override PacketType PacketType => PacketType.Unsubscribe;

    public ushort PacketId { get; init; }
    public IReadOnlyList<string> Topics { get; init; } = Array.Empty<string>();

    public virtual bool Equals(UnsubscribePacket? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        return PacketId == other.PacketId &&
               Topics.SequenceEqual(other.Topics);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(PacketId);
        hash.Add(Topics.Count);
        return hash.ToHashCode();
    }
}
