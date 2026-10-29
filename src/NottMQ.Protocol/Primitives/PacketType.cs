namespace NottMQ.Protocol.Primitives;

/// <summary>
/// MQTT Control Packet Types (OASIS MQTT v3.1.1, section 2.2.1).
/// </summary>
public enum PacketType : byte
{
    Reserved = 0,
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5, // QoS 2
    PubRel = 6, // QoS 2
    PubComp = 7, // QoS 2
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Reserved15 = 15
}
