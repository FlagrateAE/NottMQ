using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT SUBACK packet (OASIS MQTT v3.1.1, section 3.9).
/// </summary>
public record SubAckPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.SubAck;

    public ushort PacketId { get; init; }
    public IReadOnlyList<SubscribeReturnCode> ReturnCodes { get; init; } = Array.Empty<SubscribeReturnCode>();

    public virtual bool Equals(SubAckPacket? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        return PacketId == other.PacketId &&
               ReturnCodes.SequenceEqual(other.ReturnCodes);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(PacketId);
        hash.Add(ReturnCodes.Count);
        return hash.ToHashCode();
    }
}
