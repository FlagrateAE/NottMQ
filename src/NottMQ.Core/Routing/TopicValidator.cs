namespace NottMQ.Core.Routing;

/// <summary>
/// Validates MQTT v3.1.1 Topic Names and Topic Filters per OASIS section 4.7.
/// </summary>
public static class TopicValidator
{
    /// <summary>
    /// Validates an MQTT Topic Name (used in PUBLISH packets).
    /// Must be non-empty and must not contain wildcards ('+', '#') or U+0000.
    /// </summary>
    public static bool IsValidTopicName(string? topicName)
    {
        if (string.IsNullOrEmpty(topicName))
        {
            return false; // [MQTT-4.7.3-1]
        }

        foreach (var c in topicName)
        {
            if (c == '\0' || c == '+' || c == '#')
            {
                return false; // [MQTT-4.7.1-1], [MQTT-4.7.3-2]
            }
        }

        return true;
    }

    /// <summary>
    /// Validates an MQTT Topic Filter (used in SUBSCRIBE and UNSUBSCRIBE packets).
    /// Must be non-empty, must not contain U+0000, and must obey '+' and '#' placement rules.
    /// </summary>
    public static bool IsValidTopicFilter(string? topicFilter)
    {
        if (string.IsNullOrEmpty(topicFilter) || topicFilter.Contains('\0'))
        {
            return false; // [MQTT-4.7.3-1], [MQTT-4.7.3-2]
        }

        string[] levels = topicFilter.Split('/');
        for (int i = 0; i < levels.Length; i++)
        {
            var level = levels[i];

            if (level.Contains('#'))
            {
                // [MQTT-4.7.1-2] '#' must occupy an entire level and be the last level.
                if (level.Length != 1 || i != levels.Length - 1)
                {
                    return false;
                }
            }

            if (level.Contains('+') && level.Length != 1)
            {
                // [MQTT-4.7.1-3] '+' must occupy an entire level by itself.
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Splits a non-empty MQTT topic name or filter into hierarchy levels, preserving empty segments.
    /// </summary>
    public static string[] SplitLevels(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return topic.Split('/');
    }
}
