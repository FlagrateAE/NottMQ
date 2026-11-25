using NottMQ.Protocol.Primitives;

namespace NottMQ.Core.Routing;

/// <summary>
/// Thread-safe MQTT Topic Trie supporting '+' and '#' wildcards, OASIS §4.7 '$' isolation,
/// and per-client max-QoS deduplication.
/// </summary>
public sealed class TopicTrie : IDisposable
{
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private readonly TopicTrieNode _root = new();
    private readonly Dictionary<string, HashSet<string>> _clientFilters = new(StringComparer.Ordinal);

    private int _nodeCount = 1; // Includes root node.
    private int _subscriptionCount;
    private int _disposed;

    public int NodeCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _nodeCount;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    public int SubscriptionCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _subscriptionCount;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Registers or updates a client subscription for the specified topic filter.
    /// Returns <c>false</c> if the topic filter violates OASIS MQTT v3.1.1 §4.7 syntax.
    /// </summary>
    public bool TrySubscribe(string clientId, string topicFilter, MqttQoS qos)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        if (qos is < MqttQoS.AtMostOnce or > MqttQoS.ExactlyOnce)
        {
            throw new ArgumentOutOfRangeException(nameof(qos), qos, "Invalid MQTT QoS level.");
        }

        if (!TopicValidator.IsValidTopicFilter(topicFilter))
        {
            return false;
        }

        string[] levels = TopicValidator.SplitLevels(topicFilter);

        _lock.EnterWriteLock();
        try
        {
            TopicTrieNode current = _root;

            foreach (var level in levels)
            {
                if (level == "#")
                {
                    if (current.MultiLevelWildcard is null)
                    {
                        current.MultiLevelWildcard = new TopicTrieNode();
                        _nodeCount++;
                    }

                    current = current.MultiLevelWildcard;
                    break;
                }

                if (level == "+")
                {
                    if (current.SingleLevelWildcard is null)
                    {
                        current.SingleLevelWildcard = new TopicTrieNode();
                        _nodeCount++;
                    }

                    current = current.SingleLevelWildcard;
                }
                else
                {
                    current = current.GetOrAddChild(level, out bool created);
                    if (created)
                    {
                        _nodeCount++;
                    }
                }
            }

            if (current.AddOrUpdateSubscriber(clientId, qos))
            {
                _subscriptionCount++;

                if (!_clientFilters.TryGetValue(clientId, out HashSet<string>? filters))
                {
                    filters = new HashSet<string>(StringComparer.Ordinal);
                    _clientFilters[clientId] = filters;
                }

                filters.Add(topicFilter);
            }

            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes a client subscription for the specified topic filter and prunes empty trie nodes.
    /// Returns <c>true</c> if the subscription existed and was removed.
    /// </summary>
    public bool Unsubscribe(string clientId, string topicFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        if (!TopicValidator.IsValidTopicFilter(topicFilter))
        {
            return false;
        }

        _lock.EnterWriteLock();
        try
        {
            if (!UnsubscribeInternal(clientId, topicFilter))
            {
                return false;
            }

            if (_clientFilters.TryGetValue(clientId, out HashSet<string>? filters))
            {
                filters.Remove(topicFilter);
                if (filters.Count == 0)
                {
                    _clientFilters.Remove(clientId);
                }
            }

            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes all subscriptions belonging to <paramref name="clientId"/> and prunes empty trie nodes.
    /// Returns the number of subscriptions removed.
    /// </summary>
    public int UnsubscribeAll(string clientId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        _lock.EnterWriteLock();
        try
        {
            if (!_clientFilters.Remove(clientId, out HashSet<string>? filters))
            {
                return 0;
            }

            int removed = 0;
            foreach (string topicFilter in filters)
            {
                if (UnsubscribeInternal(clientId, topicFilter))
                {
                    removed++;
                }
            }

            return removed;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Finds all clients matching the specified topic name, deduplicating by ClientId
    /// with the maximum granted QoS across overlapping subscriptions ([MQTT-3.3.5-1]).
    /// </summary>
    public IReadOnlyList<TopicMatch> Match(string topicName)
    {
        ArgumentNullException.ThrowIfNull(topicName);

        if (!TopicValidator.IsValidTopicName(topicName))
        {
            return Array.Empty<TopicMatch>();
        }

        string[] levels = TopicValidator.SplitLevels(topicName);
        bool isSystemTopic = topicName[0] == '$';

        var matched = new Dictionary<string, MqttQoS>(StringComparer.Ordinal);

        _lock.EnterReadLock();
        try
        {
            MatchRecursive(_root, levels, 0, isSystemTopic, matched);
        }
        finally
        {
            _lock.ExitReadLock();
        }

        if (matched.Count == 0)
        {
            return [];
        }

        var results = new TopicMatch[matched.Count];
        int index = 0;
        foreach ((string clientId, MqttQoS qos) in matched)
        {
            results[index++] = new TopicMatch(clientId, qos);
        }

        return results;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _lock.Dispose();
        }
    }

    private bool UnsubscribeInternal(string clientId, string topicFilter)
    {
        string[] levels = TopicValidator.SplitLevels(topicFilter);
        var path = new List<(TopicTrieNode Parent, string Level, TopicTrieNode Child)>(levels.Length);
        TopicTrieNode current = _root;

        for (int i = 0; i < levels.Length; i++)
        {
            string level = levels[i];
            TopicTrieNode? next;

            if (level == "#")
            {
                next = current.MultiLevelWildcard;
                if (next is null)
                {
                    return false;
                }

                path.Add((current, level, next));
                current = next;
                break;
            }

            if (level == "+")
            {
                next = current.SingleLevelWildcard;
                if (next is null)
                {
                    return false;
                }

                path.Add((current, level, next));
                current = next;
            }
            else
            {
                if (!current.TryGetChild(level, out next) || next is null)
                {
                    return false;
                }

                path.Add((current, level, next));
                current = next;
            }
        }

        if (!current.RemoveSubscriber(clientId))
        {
            return false;
        }

        _subscriptionCount--;

        for (int i = path.Count - 1; i >= 0; i--)
        {
            (TopicTrieNode parent, string level, TopicTrieNode child) = path[i];
            if (!child.IsEmpty)
            {
                break;
            }

            if (level == "#")
            {
                parent.MultiLevelWildcard = null;
            }
            else if (level == "+")
            {
                parent.SingleLevelWildcard = null;
            }
            else
            {
                parent.RemoveChild(level);
            }

            _nodeCount--;
        }

        return true;
    }

    private static void MatchRecursive(
        TopicTrieNode node,
        string[] levels,
        int depth,
        bool isSystemTopic,
        Dictionary<string, MqttQoS> matched)
    {
        // [MQTT-4.7.2-1] Filters starting with '+' or '#' MUST NOT match '$' system topics.
        bool allowWildcard = depth > 0 || !isSystemTopic;

        if (allowWildcard && node.MultiLevelWildcard is not null)
        {
            CollectSubscribers(node.MultiLevelWildcard, matched);
        }

        if (depth == levels.Length)
        {
            CollectSubscribers(node, matched);
            return;
        }

        if (node.TryGetChild(levels[depth], out TopicTrieNode? exactChild) && exactChild is not null)
        {
            MatchRecursive(exactChild, levels, depth + 1, isSystemTopic, matched);
        }

        if (allowWildcard && node.SingleLevelWildcard is not null)
        {
            MatchRecursive(node.SingleLevelWildcard, levels, depth + 1, isSystemTopic, matched);
        }
    }

    private static void CollectSubscribers(TopicTrieNode node, Dictionary<string, MqttQoS> matched)
    {
        if (node.Subscribers is null)
        {
            return;
        }

        foreach ((string clientId, MqttQoS qos) in node.Subscribers)
        {
            if (!matched.TryGetValue(clientId, out MqttQoS existingQos) || qos > existingQos)
            {
                matched[clientId] = qos;
            }
        }
    }
}
