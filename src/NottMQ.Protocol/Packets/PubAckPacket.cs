using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT PUBACK packet (OASIS MQTT v3.1.1, section 3.4).
/// </summary>
public record PubAckPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.PubAck;

    public ushort PacketId { get; init; }
}
