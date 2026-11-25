using NottMQ.Protocol.Primitives;

namespace NottMQ.Core.Routing;

/// <summary>
/// Represents a matched subscriber client and the effective QoS granted for a topic.
/// </summary>
public readonly record struct TopicMatch(string ClientId, MqttQoS GrantedQoS);
