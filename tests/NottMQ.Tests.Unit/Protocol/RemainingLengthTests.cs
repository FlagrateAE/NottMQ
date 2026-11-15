using System.Buffers;
using FluentAssertions;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Primitives;
using Xunit;

namespace NottMQ.Tests.Unit.Protocol;

public class RemainingLengthTests
{
    [Theory]
    [InlineData(0, new byte[] { 0x00 })]
    [InlineData(127, new byte[] { 0x7F })]
    [InlineData(128, new byte[] { 0x80, 0x01 })]
    [InlineData(16_383, new byte[] { 0xFF, 0x7F })]
    [InlineData(16_384, new byte[] { 0x80, 0x80, 0x01 })]
    [InlineData(2_097_151, new byte[] { 0xFF, 0xFF, 0x7F })]
    [InlineData(2_097_152, new byte[] { 0x80, 0x80, 0x80, 0x01 })]
    [InlineData(268_435_455, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
    public void Encode_BoundaryValues_ProducesExpectedBytes(int length, byte[] expectedBytes)
    {
        var writer = new ArrayBufferWriter<byte>();
        int bytesWritten = MqttRemainingLength.Write(writer, length);

        bytesWritten.Should().Be(expectedBytes.Length);
        writer.WrittenSpan.ToArray().Should().Equal(expectedBytes);
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 0, 1)]
    [InlineData(new byte[] { 0x7F }, 127, 1)]
    [InlineData(new byte[] { 0x80, 0x01 }, 128, 2)]
    [InlineData(new byte[] { 0xFF, 0x7F }, 16_383, 2)]
    [InlineData(new byte[] { 0x80, 0x80, 0x01 }, 16_384, 3)]
    [InlineData(new byte[] { 0xFF, 0xFF, 0x7F }, 2_097_151, 3)]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x01 }, 2_097_152, 4)]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0x7F }, 268_435_455, 4)]
    public void Decode_BoundaryBytes_ProducesExpectedLength(byte[] bytes, int expectedLength, int expectedBytesRead)
    {
        var sequence = new ReadOnlySequence<byte>(bytes);
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttRemainingLength.TryRead(ref reader, out int length, out int bytesConsumed);

        success.Should().BeTrue();
        length.Should().Be(expectedLength);
        bytesConsumed.Should().Be(expectedBytesRead);
        reader.Remaining.Should().Be(0);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(268_435_456)]
    [InlineData(int.MaxValue)]
    public void Encode_OutOfRange_ThrowsArgumentOutOfRangeException(int invalidLength)
    {
        var writer = new ArrayBufferWriter<byte>();
        var act = () => MqttRemainingLength.Write(writer, invalidLength);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Decode_MoreThanFourBytesWithContinuationBit_ThrowsMqttProtocolException()
    {
        // 4 bytes with continuation bits set
        byte[] malformedBytes = [0x80, 0x80, 0x80, 0x80, 0x01];
        var sequence = new ReadOnlySequence<byte>(malformedBytes);

        var act = () =>
        {
            var reader = new SequenceReader<byte>(sequence);
            MqttRemainingLength.TryRead(ref reader, out _, out _);
        };

        act.Should().Throw<MqttProtocolException>()
            .WithMessage("*continuation bit set on byte 4*");
    }

    [Theory]
    [InlineData(new byte[] { 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80 })]
    public void Decode_IncompleteBytes_ReturnsFalseAndRewindsReader(byte[] incompleteBytes)
    {
        var sequence = new ReadOnlySequence<byte>(incompleteBytes);
        var reader = new SequenceReader<byte>(sequence);

        bool success = MqttRemainingLength.TryRead(ref reader, out int length, out int bytesConsumed);

        success.Should().BeFalse();
        length.Should().Be(0);
        bytesConsumed.Should().Be(0);
        reader.Remaining.Should().Be(incompleteBytes.Length);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(127, 1)]
    [InlineData(128, 2)]
    [InlineData(16_383, 2)]
    [InlineData(16_384, 3)]
    [InlineData(2_097_151, 3)]
    [InlineData(2_097_152, 4)]
    [InlineData(268_435_455, 4)]
    public void GetEncodedByteCount_MatchesExpected(int length, int expectedCount)
    {
        MqttRemainingLength.GetEncodedByteCount(length).Should().Be(expectedCount);
    }
}
