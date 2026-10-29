using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT PINGREQ packet (OASIS MQTT v3.1.1, section 3.12).
/// </summary>
public record PingReqPacket : MqttPacket
{
    public static readonly PingReqPacket Instance = new();
    public override PacketType PacketType => PacketType.PingReq;
}
