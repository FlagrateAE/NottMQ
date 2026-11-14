using System.Buffers;
using NottMQ.Protocol.Exceptions;

namespace NottMQ.Protocol.Primitives;

/// <summary>
/// Implements the MQTT v3.1.1 7-bit variable-byte integer encoding algorithm for Remaining Length (OASIS section 2.2.3).
/// Values range from 0 to 268,435,455 (0xFF, 0xFF, 0xFF, 0x7F).
/// </summary>
public static class MqttRemainingLength
{
    public const int MaxLength = 268_435_455;
    public const int MaxEncodedBytes = 4;

    /// <summary>
    /// Calculates the number of bytes required to encode the given length (1 to 4 bytes).
    /// </summary>
    public static int GetEncodedByteCount(int length)
    {
        if (length < 0 || length > MaxLength)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, $"Remaining Length must be between 0 and {MaxLength}.");
        }

        if (length <= 127) return 1;
        if (length <= 16_383) return 2;
        if (length <= 2_097_151) return 3;
        return 4;
    }

    /// <summary>
    /// Encodes a remaining length value into the destination buffer writer.
    /// </summary>
    public static int Write(IBufferWriter<byte> writer, int length)
    {
        if (length < 0 || length > MaxLength)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, $"Remaining Length must be between 0 and {MaxLength}.");
        }

        Span<byte> buffer = stackalloc byte[MaxEncodedBytes];
        int bytesWritten = 0;
        int value = length;

        do
        {
            byte encodedByte = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encodedByte |= 0x80;
            }
            buffer[bytesWritten++] = encodedByte;
        } while (value > 0);

        writer.Write(buffer[..bytesWritten]);
        return bytesWritten;
    }

    /// <summary>
    /// Attempts to read the remaining length from a <see cref="SequenceReader{T}"/>.
    /// If there are insufficient bytes, returns false and restores the reader's original position.
    /// If more than 4 bytes have continuation bits, throws <see cref="MqttProtocolException"/>.
    /// </summary>
    public static bool TryRead(ref SequenceReader<byte> reader, out int length, out int bytesConsumed)
    {
        SequencePosition initialPosition = reader.Position;
        int multiplier = 1;
        int value = 0;
        bytesConsumed = 0;

        while (reader.TryRead(out byte encodedByte))
        {
            bytesConsumed++;
            value += (encodedByte & 0x7F) * multiplier;

            if ((encodedByte & 0x80) == 0)
            {
                length = value;
                return true;
            }

            multiplier *= 128;
            if (bytesConsumed == MaxEncodedBytes)
            {
                // The 4th byte has continuation bit set, which is illegal
                throw new MqttProtocolException("Malformed Remaining Length: continuation bit set on byte 4.");
            }
        }

        // Not enough data in reader
        reader.Rewind(bytesConsumed);
        length = 0;
        bytesConsumed = 0;
        return false;
    }
}
