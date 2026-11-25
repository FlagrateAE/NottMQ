using FluentAssertions;
using NottMQ.Core.Routing;

namespace NottMQ.Tests.Unit.Core;

public class TopicValidatorTests
{
    [Theory]
    [InlineData("sensor/temp")]
    [InlineData("/")]
    [InlineData("/finance")]
    [InlineData("a/")]
    [InlineData("Accounts payable")]
    [InlineData(" ")]
    [InlineData("$SYS/broker/clients/connected")]
    public void IsValidTopicName_ValidNames_ReturnsTrue(string topicName)
    {
        TopicValidator.IsValidTopicName(topicName).Should().BeTrue();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("+")]
    [InlineData("#")]
    [InlineData("sensor/+")]
    [InlineData("sensor/#")]
    [InlineData("sensor/+/temp")]
    [InlineData("sensor\0temp")]
    public void IsValidTopicName_InvalidNames_ReturnsFalse(string? topicName)
    {
        TopicValidator.IsValidTopicName(topicName).Should().BeFalse();
    }

    [Theory]
    [InlineData("#")]
    [InlineData("+")]
    [InlineData("+/+")]
    [InlineData("/+")]
    [InlineData("/#")]
    [InlineData("+/#")]
    [InlineData("/")]
    [InlineData("a/")]
    [InlineData("a/#")]
    [InlineData("a/+/b")]
    [InlineData("sport/tennis/player1/#")]
    [InlineData("$SYS/#")]
    [InlineData("$SYS/+/clients")]
    public void IsValidTopicFilter_ValidFilters_ReturnsTrue(string topicFilter)
    {
        TopicValidator.IsValidTopicFilter(topicFilter).Should().BeTrue();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("sport/tennis#")]
    [InlineData("sport/#/ranking")]
    [InlineData("##")]
    [InlineData("#/")]
    [InlineData("a+")]
    [InlineData("+a")]
    [InlineData("a+b")]
    [InlineData("++")]
    [InlineData("#+")]
    [InlineData("+#")]
    [InlineData("a/\0")]
    public void IsValidTopicFilter_InvalidFilters_ReturnsFalse(string? topicFilter)
    {
        TopicValidator.IsValidTopicFilter(topicFilter).Should().BeFalse();
    }

    [Fact]
    public void SplitLevels_PreservesLeadingTrailingAndEmptySegments()
    {
        TopicValidator.SplitLevels("/finance").Should().Equal("", "finance");
        TopicValidator.SplitLevels("a/").Should().Equal("a", "");
        TopicValidator.SplitLevels("/").Should().Equal("", "");
        TopicValidator.SplitLevels("a//b").Should().Equal("a", "", "b");
        TopicValidator.SplitLevels("sport/tennis").Should().Equal("sport", "tennis");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void SplitLevels_NullOrEmpty_ThrowsArgumentException(string? topic)
    {
        Action act = () => TopicValidator.SplitLevels(topic!);
        act.Should().Throw<ArgumentException>();
    }
}
