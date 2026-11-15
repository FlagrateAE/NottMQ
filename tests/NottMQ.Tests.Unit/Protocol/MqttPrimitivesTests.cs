using System.Buffers;
using FluentAssertions;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Primitives;
using Xunit;

namespace NottMQ.Tests.Unit.Protocol;

public class MqttPrimitivesTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(ushort.MaxValue)]
    public void UInt16_WriteAndRead_RoundtripMatches(ushort value)
    {
        var writer = new ArrayBufferWriter<byte>();
        MqttPrimitives.WriteUInt16(writer, value);

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttPrimitives.TryReadUInt16(ref reader, out ushort readValue);

        success.Should().BeTrue();
        readValue.Should().Be(value);
        reader.Remaining.Should().Be(0);
    }

    [Fact]
    public void UInt16_TryReadWithInsufficientBytes_ReturnsFalse()
    {
        var sequence = new ReadOnlySequence<byte>(new byte[] { 0x01 });
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttPrimitives.TryReadUInt16(ref reader, out ushort value);

        success.Should().BeFalse();
        value.Should().Be(0);
        reader.Remaining.Should().Be(1);
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("test/topic/name")]
    [InlineData("Український текст & UTF-8 🚀")]
    public void String_WriteAndRead_RoundtripMatches(string original)
    {
        var writer = new ArrayBufferWriter<byte>();
        int written = MqttPrimitives.WriteString(writer, original);

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttPrimitives.TryReadString(ref reader, out string? readString);

        success.Should().BeTrue();
        readString.Should().Be(original);
        reader.Remaining.Should().Be(0);
    }

    [Fact]
    public void String_ContainingNullChar_ThrowsMqttProtocolExceptionOnWrite()
    {
        var writer = new ArrayBufferWriter<byte>();
        var act = () => MqttPrimitives.WriteString(writer, "hello\0world");

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-1.5.3-2]*");
    }

    [Fact]
    public void String_ContainingNullChar_ThrowsMqttProtocolExceptionOnRead()
    {
        var writer = new ArrayBufferWriter<byte>();
        // Manually write string with null byte
        MqttPrimitives.WriteUInt16(writer, 5);
        writer.Write(new byte[] { (byte)'a', 0x00, (byte)'b', (byte)'c', (byte)'d' });

        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);

        var act = () =>
        {
            var reader = new SequenceReader<byte>(sequence);
            MqttPrimitives.TryReadString(ref reader, out _);
        };

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*[MQTT-1.5.3-2]*");
    }

    [Fact]
    public void String_InsufficientBytesForLength_ReturnsFalse()
    {
        var sequence = new ReadOnlySequence<byte>(new byte[] { 0x00 });
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttPrimitives.TryReadString(ref reader, out string? value);

        success.Should().BeFalse();
        value.Should().BeNull();
    }

    [Fact]
    public void String_InsufficientBytesForPayload_ReturnsFalseAndRewinds()
    {
        // Declares 10 bytes length, but only 3 bytes follow
        var sequence = new ReadOnlySequence<byte>(new byte[] { 0x00, 0x0A, 0x41, 0x42, 0x43 });
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttPrimitives.TryReadString(ref reader, out string? value);

        success.Should().BeFalse();
        value.Should().BeNull();
        reader.Remaining.Should().Be(5);
    }
}
