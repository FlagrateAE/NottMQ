using NottMQ.Core.Routing;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Stress;

/// <summary>
/// Simple deterministic reference oracle backed by a plain list under a lock
/// for differential verification of <see cref="TopicTrie"/>.
/// </summary>
public sealed class SequentialRoutingOracle
{
    private readonly Lock _gate = new();
    private readonly List<SubscriptionEntry> _subscriptions = [];

    public int SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptions.Count;
            }
        }
    }

    public bool TrySubscribe(string clientId, string topicFilter, MqttQoS qos)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        if (!TopicValidator.IsValidTopicFilter(topicFilter))
        {
            return false;
        }

        lock (_gate)
        {
            for (int i = 0; i < _subscriptions.Count; i++)
            {
                SubscriptionEntry entry = _subscriptions[i];
                if (string.Equals(entry.ClientId, clientId, StringComparison.Ordinal) &&
                    string.Equals(entry.TopicFilter, topicFilter, StringComparison.Ordinal))
                {
                    _subscriptions[i] = entry with { QoS = qos };
                    return true;
                }
            }

            _subscriptions.Add(new SubscriptionEntry(clientId, topicFilter, qos));
            return true;
        }
    }

    public bool Unsubscribe(string clientId, string topicFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        if (!TopicValidator.IsValidTopicFilter(topicFilter))
        {
            return false;
        }

        lock (_gate)
        {
            for (int i = 0; i < _subscriptions.Count; i++)
            {
                SubscriptionEntry entry = _subscriptions[i];
                if (string.Equals(entry.ClientId, clientId, StringComparison.Ordinal) &&
                    string.Equals(entry.TopicFilter, topicFilter, StringComparison.Ordinal))
                {
                    _subscriptions.RemoveAt(i);
                    return true;
                }
            }

            return false;
        }
    }

    public int UnsubscribeAll(string clientId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        lock (_gate)
        {
            return _subscriptions.RemoveAll(e =>
                string.Equals(e.ClientId, clientId, StringComparison.Ordinal));
        }
    }

    public IReadOnlyList<TopicMatch> Match(string topicName)
    {
        ArgumentNullException.ThrowIfNull(topicName);

        if (!TopicValidator.IsValidTopicName(topicName))
        {
            return Array.Empty<TopicMatch>();
        }

        var matched = new Dictionary<string, MqttQoS>(StringComparer.Ordinal);

        lock (_gate)
        {
            foreach (SubscriptionEntry entry in _subscriptions)
            {
                if (!MatchesFilter(entry.TopicFilter, topicName))
                {
                    continue;
                }

                if (!matched.TryGetValue(entry.ClientId, out MqttQoS existing) || entry.QoS > existing)
                {
                    matched[entry.ClientId] = entry.QoS;
                }
            }
        }

        return matched.Select(kvp => new TopicMatch(kvp.Key, kvp.Value)).ToArray();
    }

    public static bool MatchesFilter(string filter, string topic)
    {
        if (!TopicValidator.IsValidTopicFilter(filter) || !TopicValidator.IsValidTopicName(topic))
        {
            return false;
        }

        // [MQTT-4.7.2-1]
        if (topic[0] == '$' && (filter[0] == '+' || filter[0] == '#'))
        {
            return false;
        }

        string[] filterLevels = filter.Split('/');
        string[] topicLevels = topic.Split('/');

        for (int i = 0; i < filterLevels.Length; i++)
        {
            string f = filterLevels[i];

            if (f == "#")
            {
                return true;
            }

            if (i >= topicLevels.Length)
            {
                return false;
            }

            if (f != "+" && !string.Equals(f, topicLevels[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return filterLevels.Length == topicLevels.Length;
    }

    private readonly record struct SubscriptionEntry(string ClientId, string TopicFilter, MqttQoS QoS);
}
