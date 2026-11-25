using FluentAssertions;
using NottMQ.Core.Routing;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Stress;

public class TopicTrieDifferentialTests
{
    private static readonly string[] Clients = Enumerable.Range(0, 20)
        .Select(i => $"client-{i}")
        .ToArray();

    private static readonly string[] TopicFilters =
    [
        "sport/tennis/player1",
        "sport/tennis/player1/#",
        "sport/tennis/#",
        "sport/+/player1",
        "+/tennis/+",
        "sport/+/+",
        "+/+",
        "+",
        "#",
        "/#",
        "/+",
        "/",
        "a/",
        "a//b",
        "a/+/b",
        "sensors/room1/temp",
        "sensors/+/temp",
        "sensors/#",
        "$SYS/#",
        "$SYS/broker/+",
        "$SYS/broker/clients",
        "+/broker/clients",
        // Invalid filters to ensure both reject identically:
        "sport/#/ranking",
        "a+"
    ];

    private static readonly string[] PublishTopics =
    [
        "sport/tennis/player1",
        "sport/tennis/player1/ranking",
        "sport/tennis/player1/score/wimbledon",
        "sport/tennis/player2",
        "sport/football/player1",
        "finance",
        "/finance",
        "/",
        "a/",
        "a",
        "a//b",
        "a/x/b",
        "sensors/room1/temp",
        "sensors/room2/temp",
        "sensors/room1/humidity",
        "$SYS",
        "$SYS/broker",
        "$SYS/broker/clients",
        "$SYS/broker/load",
        "other/broker/clients"
    ];

    private static readonly MqttQoS[] QoSLevels =
    [
        MqttQoS.AtMostOnce,
        MqttQoS.AtLeastOnce,
        MqttQoS.ExactlyOnce
    ];

    [Theory]
    [InlineData(1337)]
    [InlineData(42)]
    [InlineData(2026)]
    public void RandomizedOperations_MatchSequentialOracleAndPruneCompletely(int seed)
    {
        var rng = new Random(seed);
        using var trie = new TopicTrie();
        var oracle = new SequentialRoutingOracle();

        const int operations = 5_000;
        for (int step = 0; step < operations; step++)
        {
            int roll = rng.Next(100);
            string client = Clients[rng.Next(Clients.Length)];

            if (roll < 45)
            {
                string filter = TopicFilters[rng.Next(TopicFilters.Length)];
                MqttQoS qos = QoSLevels[rng.Next(QoSLevels.Length)];

                bool trieResult = trie.TrySubscribe(client, filter, qos);
                bool oracleResult = oracle.TrySubscribe(client, filter, qos);
                trieResult.Should().Be(oracleResult, $"step {step}: subscribe {client} -> {filter}");
            }
            else if (roll < 70)
            {
                string filter = TopicFilters[rng.Next(TopicFilters.Length)];

                bool trieResult = trie.Unsubscribe(client, filter);
                bool oracleResult = oracle.Unsubscribe(client, filter);
                trieResult.Should().Be(oracleResult, $"step {step}: unsubscribe {client} -> {filter}");
            }
            else if (roll < 75)
            {
                int trieRemoved = trie.UnsubscribeAll(client);
                int oracleRemoved = oracle.UnsubscribeAll(client);
                trieRemoved.Should().Be(oracleRemoved, $"step {step}: unsubscribeAll {client}");
            }
            else
            {
                string topic = PublishTopics[rng.Next(PublishTopics.Length)];
                IReadOnlyList<TopicMatch> actual = trie.Match(topic);
                IReadOnlyList<TopicMatch> expected = oracle.Match(topic);

                actual.Should().BeEquivalentTo(expected, $"step {step}: match '{topic}'");
            }

            trie.SubscriptionCount.Should().Be(oracle.SubscriptionCount, $"step {step}: subscription count");
            trie.NodeCount.Should().BeGreaterThanOrEqualTo(1);
        }

        // Full corpus verification after randomized mutations.
        foreach (string topic in PublishTopics)
        {
            trie.Match(topic).Should().BeEquivalentTo(oracle.Match(topic), $"final corpus check for '{topic}'");
        }

        // Unsubscribing every client must completely prune the trie back to root only.
        foreach (string client in Clients)
        {
            trie.UnsubscribeAll(client).Should().Be(oracle.UnsubscribeAll(client));
        }

        trie.SubscriptionCount.Should().Be(0);
        trie.NodeCount.Should().Be(1);
    }
}
