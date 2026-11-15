using System.Buffers;
using FluentAssertions;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;
using Xunit;

namespace NottMQ.Tests.Unit.Protocol;

public class SpecConformanceTests
{
    [Theory]
    [InlineData(PacketType.Connect, 0x01)]
    [InlineData(PacketType.ConnAck, 0x02)]
    [InlineData(PacketType.PubAck, 0x04)]
    [InlineData(PacketType.SubAck, 0x08)]
    [InlineData(PacketType.UnsubAck, 0x01)]
    [InlineData(PacketType.PingReq, 0x02)]
    [InlineData(PacketType.PingResp, 0x04)]
    [InlineData(PacketType.Disconnect, 0x08)]
    public void NonZero_ReservedFlags_ThrowsProtocolException(PacketType type, byte invalidFlag)
    {
        byte firstByte = (byte)(((byte)type << 4) | (invalidFlag & 0x0F));
        byte[] bytes = [firstByte, 0x00];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-2.2.2-2]*");
    }

    [Theory]
    [InlineData(0x00)]
    [InlineData(0x01)]
    [InlineData(0x03)]
    public void Subscribe_InvalidReservedFlags_ThrowsProtocolException(byte invalidFlags)
    {
        byte firstByte = (byte)(((byte)PacketType.Subscribe << 4) | (invalidFlags & 0x0F));
        byte[] bytes = [firstByte, 0x05, 0x00, 0x01, 0x00, 0x01, (byte)'a', 0x00];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.8.1-1]*");
    }

    [Theory]
    [InlineData(0x00)]
    [InlineData(0x01)]
    [InlineData(0x03)]
    public void Unsubscribe_InvalidReservedFlags_ThrowsProtocolException(byte invalidFlags)
    {
        byte firstByte = (byte)(((byte)PacketType.Unsubscribe << 4) | (invalidFlags & 0x0F));
        byte[] bytes = [firstByte, 0x05, 0x00, 0x01, 0x00, 0x01, (byte)'a'];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.10.1-1]*");
    }

    [Fact]
    public void Publish_QoS3_ThrowsProtocolException()
    {
        // bits 2-1 = 11 (QoS 3)
        byte firstByte = (byte)(((byte)PacketType.Publish << 4) | 0x06);
        byte[] bytes = [firstByte, 0x04, 0x00, 0x01, (byte)'a', 0x00];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.3.1-4]*");
    }

    [Fact]
    public void Publish_QoS0_With_DuplicateFlagSet_ThrowsProtocolException()
    {
        // QoS 0, DUP = 1 (bit 3) -> flags = 0x08
        byte firstByte = (byte)(((byte)PacketType.Publish << 4) | 0x08);
        byte[] bytes = [firstByte, 0x03, 0x00, 0x01, (byte)'a'];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.3.1-2]*");
    }

    [Theory]
    [InlineData("sensor/+")]
    [InlineData("sensor/#")]
    [InlineData("a/+/c")]
    public void Publish_WithWildcardsInTopic_ThrowsProtocolException(string invalidTopic)
    {
        var publish = new PublishPacket
        {
            Topic = "sensor/temp",
            QualityOfService = MqttQoS.AtMostOnce
        };

        var writer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodePublish(publish, writer);

        // Replace topic in bytes
        var customPublish = new PublishPacket
        {
            Topic = invalidTopic,
            QualityOfService = MqttQoS.AtMostOnce
        };

        // We can manually craft bytes or construct
        var customWriter = new ArrayBufferWriter<byte>();
        // Write fixed header
        customWriter.Write(new byte[] { 0x30 });
        // Body writer
        var bodyWriter = new ArrayBufferWriter<byte>();
        MqttPrimitives.WriteString(bodyWriter, invalidTopic);
        MqttRemainingLength.Write(customWriter, bodyWriter.WrittenCount);
        customWriter.Write(bodyWriter.WrittenSpan);

        var sequence = new ReadOnlySequence<byte>(customWriter.WrittenMemory);
        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-4.7.1-1]*");
    }

    [Fact]
    public void Connect_ReservedBitNotZero_ThrowsProtocolException()
    {
        var writer = new ArrayBufferWriter<byte>();
        writer.Write(new byte[] { 0x10 }); // Connect Fixed Header

        var body = new ArrayBufferWriter<byte>();
        MqttPrimitives.WriteString(body, "MQTT");
        body.Write(new byte[] { 0x04 }); // Protocol level 4
        body.Write(new byte[] { 0x03 }); // CleanSession (0x02) + Reserved bit 0 set (0x01) -> 0x03
        MqttPrimitives.WriteUInt16(body, 60);
        MqttPrimitives.WriteString(body, "client");

        MqttRemainingLength.Write(writer, body.WrittenCount);
        writer.Write(body.WrittenSpan);

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.1.2-3]*");
    }

    [Fact]
    public void Connect_PasswordWithoutUsername_ThrowsProtocolException()
    {
        var writer = new ArrayBufferWriter<byte>();
        writer.Write(new byte[] { 0x10 }); // Connect Fixed Header

        var body = new ArrayBufferWriter<byte>();
        MqttPrimitives.WriteString(body, "MQTT");
        body.Write(new byte[] { 0x04 }); // Protocol level 4
        body.Write(new byte[] { 0x42 }); // CleanSession (0x02) + Password (0x40), Username not set
        MqttPrimitives.WriteUInt16(body, 60);
        MqttPrimitives.WriteString(body, "client");
        MqttPrimitives.WriteString(body, "password");

        MqttRemainingLength.Write(writer, body.WrittenCount);
        writer.Write(body.WrittenSpan);

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.1.2-22]*");
    }

    [Fact]
    public void ConnAck_ReservedBitsSet_ThrowsProtocolException()
    {
        // Byte 1 has bit 2 set (0x04) which is reserved
        byte[] bytes = [0x20, 0x02, 0x04, 0x00];
        var sequence = new ReadOnlySequence<byte>(bytes);

        var act = () => MqttDecoder.TryDecode(ref sequence, out _);

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-3.2.2-2]*");
    }
}
