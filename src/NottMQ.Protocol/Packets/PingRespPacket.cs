using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT PINGRESP packet (OASIS MQTT v3.1.1, section 3.13).
/// </summary>
public record PingRespPacket : MqttPacket
{
    public static readonly PingRespPacket Instance = new();
    public override PacketType PacketType => PacketType.PingResp;
}
