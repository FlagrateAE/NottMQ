using NottMQ.Protocol.Primitives;

namespace NottMQ.Core.Routing;

internal sealed class TopicTrieNode
{
    public Dictionary<string, TopicTrieNode>? Children { get; private set; }
    public TopicTrieNode? SingleLevelWildcard { get; set; }
    public TopicTrieNode? MultiLevelWildcard { get; set; }
    public Dictionary<string, MqttQoS>? Subscribers { get; private set; }

    public bool IsEmpty =>
        (Subscribers is null || Subscribers.Count == 0) &&
        (Children is null || Children.Count == 0) &&
        SingleLevelWildcard is null &&
        MultiLevelWildcard is null;

    public TopicTrieNode GetOrAddChild(string segment, out bool created)
    {
        Children ??= new Dictionary<string, TopicTrieNode>(StringComparer.Ordinal);
        if (!Children.TryGetValue(segment, out TopicTrieNode? child))
        {
            child = new TopicTrieNode();
            Children[segment] = child;
            created = true;
            return child;
        }

        created = false;
        return child;
    }

    public bool TryGetChild(string segment, out TopicTrieNode? child)
    {
        if (Children is null)
        {
            child = null;
            return false;
        }

        return Children.TryGetValue(segment, out child);
    }

    public void RemoveChild(string segment)
    {
        if (Children is null)
        {
            return;
        }

        Children.Remove(segment);
        if (Children.Count == 0)
        {
            Children = null;
        }
    }

    public bool AddOrUpdateSubscriber(string clientId, MqttQoS qos)
    {
        Subscribers ??= new Dictionary<string, MqttQoS>(StringComparer.Ordinal);
        bool isNew = !Subscribers.ContainsKey(clientId);
        Subscribers[clientId] = qos;
        return isNew;
    }

    public bool RemoveSubscriber(string clientId)
    {
        if (Subscribers is null || !Subscribers.Remove(clientId))
        {
            return false;
        }

        if (Subscribers.Count == 0)
        {
            Subscribers = null;
        }

        return true;
    }
}
