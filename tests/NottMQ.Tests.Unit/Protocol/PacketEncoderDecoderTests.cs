using System.Buffers;
using System.Text;
using FluentAssertions;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;
using Xunit;

namespace NottMQ.Tests.Unit.Protocol;

public class PacketEncoderDecoderTests
{
    private static T Roundtrip<T>(T packet) where T : MqttPacket
    {
        var writer = new ArrayBufferWriter<byte>();
        MqttEncoder.Encode(packet, writer);

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        bool success = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        success.Should().BeTrue();
        decodedPacket.Should().NotBeNull();
        decodedPacket.Should().BeOfType<T>();
        sequence.Length.Should().Be(0);

        return (T)decodedPacket!;
    }

    [Fact]
    public void ConnectPacket_Roundtrip_Matches()
    {
        var original = new ConnectPacket
        {
            ProtocolName = "MQTT",
            ProtocolLevel = 4,
            CleanSession = true,
            KeepAliveSeconds = 30,
            ClientId = "client-12345",
            Username = "admin",
            Password = "secret-password",
            HasWill = true,
            WillQoS = MqttQoS.AtLeastOnce,
            WillRetain = false,
            WillTopic = "status/will",
            WillMessage = Encoding.UTF8.GetBytes("offline")
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void ConnAckPacket_Roundtrip_Matches()
    {
        var original = new ConnAckPacket
        {
            SessionPresent = true,
            ReturnCode = ConnectReturnCode.Accepted
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void PublishPacket_QoS0_Roundtrip_Matches()
    {
        var original = new PublishPacket
        {
            Topic = "sensors/temperature",
            QualityOfService = MqttQoS.AtMostOnce,
            Duplicate = false,
            Retain = true,
            Payload = Encoding.UTF8.GetBytes("24.5C")
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void PublishPacket_QoS1_Roundtrip_Matches()
    {
        var original = new PublishPacket
        {
            Topic = "sensors/temperature",
            PacketId = 42,
            QualityOfService = MqttQoS.AtLeastOnce,
            Duplicate = true,
            Retain = false,
            Payload = Encoding.UTF8.GetBytes("25.0C")
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void PublishPacket_EmptyPayload_Roundtrip_Matches()
    {
        var original = new PublishPacket
        {
            Topic = "sensors/ping",
            QualityOfService = MqttQoS.AtMostOnce,
            Payload = ReadOnlyMemory<byte>.Empty
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void PubAckPacket_Roundtrip_Matches()
    {
        var original = new PubAckPacket { PacketId = 1234 };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void SubscribePacket_Roundtrip_Matches()
    {
        var original = new SubscribePacket
        {
            PacketId = 101,
            Subscriptions = new List<TopicSubscription>
            {
                new("home/livingroom/temp", MqttQoS.AtLeastOnce),
                new("home/bedroom/+", MqttQoS.AtMostOnce)
            }
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void SubAckPacket_Roundtrip_Matches()
    {
        var original = new SubAckPacket
        {
            PacketId = 101,
            ReturnCodes = new List<SubscribeReturnCode>
            {
                SubscribeReturnCode.SuccessQoS1,
                SubscribeReturnCode.SuccessQoS0,
                SubscribeReturnCode.Failure
            }
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void UnsubscribePacket_Roundtrip_Matches()
    {
        var original = new UnsubscribePacket
        {
            PacketId = 55,
            Topics = new List<string> { "sensor/temp", "sensor/humidity" }
        };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void UnsubAckPacket_Roundtrip_Matches()
    {
        var original = new UnsubAckPacket { PacketId = 55 };

        var decoded = Roundtrip(original);
        decoded.Should().Be(original);
    }

    [Fact]
    public void PingReqPacket_Roundtrip_Matches()
    {
        var decoded = Roundtrip(PingReqPacket.Instance);
        decoded.Should().Be(PingReqPacket.Instance);
    }

    [Fact]
    public void PingRespPacket_Roundtrip_Matches()
    {
        var decoded = Roundtrip(PingRespPacket.Instance);
        decoded.Should().Be(PingRespPacket.Instance);
    }

    [Fact]
    public void DisconnectPacket_Roundtrip_Matches()
    {
        var decoded = Roundtrip(DisconnectPacket.Instance);
        decoded.Should().Be(DisconnectPacket.Instance);
    }
}
