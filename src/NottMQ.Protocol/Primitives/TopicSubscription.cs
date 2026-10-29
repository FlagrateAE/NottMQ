namespace NottMQ.Protocol.Primitives;

/// <summary>
/// Represents a requested topic filter and its requested Quality of Service in a SUBSCRIBE packet.
/// </summary>
public readonly record struct TopicSubscription(string TopicFilter, MqttQoS RequestedQoS);
