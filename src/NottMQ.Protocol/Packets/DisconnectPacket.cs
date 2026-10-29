using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT DISCONNECT packet (OASIS MQTT v3.1.1, section 3.14).
/// </summary>
public record DisconnectPacket : MqttPacket
{
    public static readonly DisconnectPacket Instance = new();
    public override PacketType PacketType => PacketType.Disconnect;
}
