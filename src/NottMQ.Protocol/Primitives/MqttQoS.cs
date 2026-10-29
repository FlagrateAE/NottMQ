namespace NottMQ.Protocol.Primitives;

/// <summary>
/// MQTT Quality of Service levels (OASIS MQTT v3.1.1, section 4.3).
/// </summary>
public enum MqttQoS : byte
{
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2
}