using FluentAssertions;
using NottMQ.Core.Routing;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Unit.Core;

public class TopicTrieTests
{
    [Fact]
    public void Match_ExactTopics_IsCaseSensitiveAndPreservesSlashes()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "Sport/Tennis", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c2", "/finance", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("c3", "a/", MqttQoS.AtMostOnce).Should().BeTrue();

        trie.Match("Sport/Tennis").Should().Equal(new TopicMatch("c1", MqttQoS.AtMostOnce));
        trie.Match("sport/tennis").Should().BeEmpty();
        trie.Match("/finance").Should().Equal(new TopicMatch("c2", MqttQoS.AtLeastOnce));
        trie.Match("finance").Should().BeEmpty();
        trie.Match("a/").Should().Equal(new TopicMatch("c3", MqttQoS.AtMostOnce));
        trie.Match("a").Should().BeEmpty();
    }

    [Fact]
    public void Match_SingleLevelWildcard_FollowsOasisLevelRules()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("plusOnly", "+", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("plusPlus", "+/+", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("slashPlus", "/+", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("midPlus", "a/+/c", MqttQoS.AtMostOnce).Should().BeTrue();

        trie.Match("finance")
            .Should().BeEquivalentTo([new TopicMatch("plusOnly", MqttQoS.AtMostOnce)]);

        // [MQTT-4.7.1.3] "+" does not match "/finance", while "+/+" and "/+" do.
        trie.Match("/finance")
            .Should().BeEquivalentTo([
                new TopicMatch("plusPlus", MqttQoS.AtMostOnce),
                new TopicMatch("slashPlus", MqttQoS.AtLeastOnce)
            ]);

        trie.Match("/")
            .Should().BeEquivalentTo([
                new TopicMatch("plusPlus", MqttQoS.AtMostOnce),
                new TopicMatch("slashPlus", MqttQoS.AtLeastOnce)
            ]);

        trie.Match("a/b/c").Should().Equal(new TopicMatch("midPlus", MqttQoS.AtMostOnce));
        trie.Match("a//c").Should().Equal(new TopicMatch("midPlus", MqttQoS.AtMostOnce));
        trie.Match("a/c").Should().Equal(new TopicMatch("plusPlus", MqttQoS.AtMostOnce));
        trie.Match("a/b/c/d").Should().BeEmpty();
    }

    [Fact]
    public void Match_MultiLevelWildcard_MatchesParentAndAllDescendants()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("player1All", "sport/tennis/player1/#", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("slashHash", "/#", MqttQoS.AtMostOnce).Should().BeTrue();

        // [MQTT-4.7.1.2] "sport/tennis/player1/#" matches parent level and deeper levels.
        trie.Match("sport/tennis/player1").Should().Equal(new TopicMatch("player1All", MqttQoS.AtLeastOnce));
        trie.Match("sport/tennis/player1/ranking").Should().Equal(new TopicMatch("player1All", MqttQoS.AtLeastOnce));
        trie.Match("sport/tennis/player1/score/wimbledon").Should().Equal(new TopicMatch("player1All", MqttQoS.AtLeastOnce));
        trie.Match("sport/tennis").Should().BeEmpty();

        trie.Match("/").Should().Equal(new TopicMatch("slashHash", MqttQoS.AtMostOnce));
        trie.Match("/a/b").Should().Equal(new TopicMatch("slashHash", MqttQoS.AtMostOnce));
        trie.Match("a").Should().BeEmpty();
    }

    [Fact]
    public void Match_SystemDollarTopics_AreIsolatedFromLeadingWildcards()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("rootHash", "#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("rootPlus", "+", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("leadingPlus", "+/monitor/Clients", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("leadingPlusHash", "+/#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("sysHash", "$SYS/#", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("sysMidPlus", "$SYS/monitor/+", MqttQoS.AtMostOnce).Should().BeTrue();

        // [MQTT-4.7.2-1] Filters starting with '+' or '#' MUST NOT match '$' topics.
        trie.Match("$SYS")
            .Should().BeEquivalentTo([new TopicMatch("sysHash", MqttQoS.AtLeastOnce)]);

        trie.Match("$SYS/monitor/Clients")
            .Should().BeEquivalentTo([
                new TopicMatch("sysHash", MqttQoS.AtLeastOnce),
                new TopicMatch("sysMidPlus", MqttQoS.AtMostOnce)
            ]);
    }

    [Fact]
    public void Match_OverlappingSubscriptions_DeduplicatesClientWithMaxQoS()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "sport/+", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "sport/tennis", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c2", "sport/#", MqttQoS.AtMostOnce).Should().BeTrue();

        IReadOnlyList<TopicMatch> matches = trie.Match("sport/tennis");

        matches.Should().BeEquivalentTo([
            new TopicMatch("c1", MqttQoS.AtLeastOnce),
            new TopicMatch("c2", MqttQoS.AtMostOnce)
        ]);
    }

    [Fact]
    public void TrySubscribe_DuplicateClientFilter_UpdatesQoSWithoutDuplicatingCount()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "sensors/temp", MqttQoS.AtMostOnce).Should().BeTrue();
        int initialNodes = trie.NodeCount;

        trie.TrySubscribe("c1", "sensors/temp", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.SubscriptionCount.Should().Be(1);
        trie.NodeCount.Should().Be(initialNodes);
        trie.Match("sensors/temp").Should().Equal(new TopicMatch("c1", MqttQoS.AtLeastOnce));

        // Downgrading QoS also updates stored subscription.
        trie.TrySubscribe("c1", "sensors/temp", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.SubscriptionCount.Should().Be(1);
        trie.Match("sensors/temp").Should().Equal(new TopicMatch("c1", MqttQoS.AtMostOnce));
    }

    [Theory]
    [InlineData("")]
    [InlineData("a/#/b")]
    [InlineData("a/b#")]
    [InlineData("a+b")]
    public void TrySubscribe_InvalidTopicFilter_ReturnsFalseWithoutModifyingTrie(string invalidFilter)
    {
        using var trie = new TopicTrie();

        trie.TrySubscribe("c1", invalidFilter, MqttQoS.AtMostOnce).Should().BeFalse();
        trie.NodeCount.Should().Be(1);
        trie.SubscriptionCount.Should().Be(0);
    }

    [Fact]
    public void TrySubscribe_InvalidQoS_ThrowsArgumentOutOfRangeException()
    {
        using var trie = new TopicTrie();

        Action act = () => trie.TrySubscribe("c1", "a/b", (MqttQoS)3);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("a/+")]
    [InlineData("a/#")]
    public void Match_InvalidTopicName_ReturnsEmpty(string invalidTopic)
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "#", MqttQoS.AtMostOnce).Should().BeTrue();

        trie.Match(invalidTopic).Should().BeEmpty();
    }

    [Fact]
    public void Unsubscribe_PrunesEmptyBranchesCompletely()
    {
        using var trie = new TopicTrie();
        trie.NodeCount.Should().Be(1);

        trie.TrySubscribe("c1", "a/b/c/d/e", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.NodeCount.Should().Be(6);
        trie.SubscriptionCount.Should().Be(1);

        trie.Unsubscribe("c1", "a/b/c/d/e").Should().BeTrue();
        trie.NodeCount.Should().Be(1);
        trie.SubscriptionCount.Should().Be(0);
        trie.Match("a/b/c/d/e").Should().BeEmpty();
    }

    [Fact]
    public void Unsubscribe_SharedPrefixAndIntermediateSubscriptions_PrunesOnlyEmptyNodes()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "a/b/c", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c2", "a/b/d", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.NodeCount.Should().Be(5); // root, a, b, c, d

        trie.Unsubscribe("c1", "a/b/c").Should().BeTrue();
        trie.NodeCount.Should().Be(4); // root, a, b, d
        trie.SubscriptionCount.Should().Be(1);
        trie.Match("a/b/c").Should().BeEmpty();
        trie.Match("a/b/d").Should().Equal(new TopicMatch("c2", MqttQoS.AtLeastOnce));

        trie.Unsubscribe("c2", "a/b/d").Should().BeTrue();
        trie.NodeCount.Should().Be(1);
        trie.SubscriptionCount.Should().Be(0);
    }

    [Fact]
    public void Unsubscribe_WildcardsAndIntermediateParent_PrunesCleanly()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "a/+", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "a/+/b", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "a/#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.NodeCount.Should().Be(5); // root, a, +, b, #

        // Unsubscribing parent "a/+" must keep "a/+/b" intact.
        trie.Unsubscribe("c1", "a/+").Should().BeTrue();
        trie.NodeCount.Should().Be(5);
        trie.Match("a/x/b").Should().Equal(new TopicMatch("c1", MqttQoS.AtLeastOnce));

        trie.Unsubscribe("c1", "a/+/b").Should().BeTrue();
        trie.NodeCount.Should().Be(3); // root, a, #

        trie.Unsubscribe("c1", "a/#").Should().BeTrue();
        trie.NodeCount.Should().Be(1);
        trie.SubscriptionCount.Should().Be(0);
    }

    [Fact]
    public void Unsubscribe_NonExistentOrInvalid_ReturnsFalse()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "a/b", MqttQoS.AtMostOnce).Should().BeTrue();

        trie.Unsubscribe("c1", "a/#/b").Should().BeFalse();
        trie.Unsubscribe("c1", "a/c").Should().BeFalse();
        trie.Unsubscribe("c1", "x/y/z").Should().BeFalse();
        trie.Unsubscribe("c2", "a/b").Should().BeFalse();
        trie.Unsubscribe("c1", "a/+").Should().BeFalse();
        trie.Unsubscribe("c1", "a/#").Should().BeFalse();

        trie.SubscriptionCount.Should().Be(1);
        trie.NodeCount.Should().Be(3);
    }

    [Fact]
    public void UnsubscribeAll_RemovesAllClientFiltersAndPrunesNodes()
    {
        using var trie = new TopicTrie();
        trie.TrySubscribe("c1", "a/b", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "x/+/z", MqttQoS.AtLeastOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "m/#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c1", "#", MqttQoS.AtMostOnce).Should().BeTrue();
        trie.TrySubscribe("c2", "a/b", MqttQoS.AtMostOnce).Should().BeTrue();

        int removed = trie.UnsubscribeAll("c1");

        removed.Should().Be(4);
        trie.SubscriptionCount.Should().Be(1);
        trie.NodeCount.Should().Be(3); // root, a, b (c2)
        trie.Match("a/b").Should().Equal(new TopicMatch("c2", MqttQoS.AtMostOnce));
        trie.Match("x/y/z").Should().BeEmpty();
        trie.Match("m/n").Should().BeEmpty();

        trie.UnsubscribeAll("c1").Should().Be(0);
        trie.UnsubscribeAll("c2").Should().Be(1);
        trie.NodeCount.Should().Be(1);
        trie.SubscriptionCount.Should().Be(0);
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_IsIdempotent()
    {
        var trie = new TopicTrie();
        trie.Dispose();
        trie.Dispose();
    }
}
