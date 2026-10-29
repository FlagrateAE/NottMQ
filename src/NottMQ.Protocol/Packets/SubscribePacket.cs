using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT SUBSCRIBE packet (OASIS MQTT v3.1.1, section 3.8).
/// </summary>
public record SubscribePacket : MqttPacket
{
    public override PacketType PacketType => PacketType.Subscribe;

    public ushort PacketId { get; init; }
    public IReadOnlyList<TopicSubscription> Subscriptions { get; init; } = Array.Empty<TopicSubscription>();

    public virtual bool Equals(SubscribePacket? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        return PacketId == other.PacketId &&
               Subscriptions.SequenceEqual(other.Subscriptions);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(PacketId);
        hash.Add(Subscriptions.Count);
        return hash.ToHashCode();
    }
}
