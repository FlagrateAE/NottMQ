using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// Abstract base record for all MQTT control packets.
/// </summary>
public abstract record MqttPacket
{
    public abstract PacketType PacketType { get; }
}
