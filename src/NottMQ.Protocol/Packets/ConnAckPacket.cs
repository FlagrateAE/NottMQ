using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT CONNACK packet (OASIS MQTT v3.1.1, section 3.2).
/// </summary>
public record ConnAckPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.ConnAck;

    public bool SessionPresent { get; init; }
    public ConnectReturnCode ReturnCode { get; init; } = ConnectReturnCode.Accepted;
}
