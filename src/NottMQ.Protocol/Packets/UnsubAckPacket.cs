using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT UNSUBACK packet (OASIS MQTT v3.1.1, section 3.11).
/// </summary>
public record UnsubAckPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.UnsubAck;

    public ushort PacketId { get; init; }
}
