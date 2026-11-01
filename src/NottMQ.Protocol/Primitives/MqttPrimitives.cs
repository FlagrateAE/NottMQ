using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using NottMQ.Protocol.Exceptions;

namespace NottMQ.Protocol.Primitives;

/// <summary>
/// Common binary serialization and deserialization primitives defined by OASIS MQTT v3.1.1.
/// </summary>
public static class MqttPrimitives
{
    private static readonly UTF8Encoding Utf8Encoding =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// Writes a 16-bit unsigned integer in Big-Endian network byte order.
    /// </summary>
    public static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> span = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        writer.Advance(2);
    }

    /// <summary>
    /// Attempts to read a 16-bit unsigned integer in Big-Endian network byte order.
    /// </summary>
    public static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        if (reader.TryReadBigEndian(out short signedValue))
        {
            value = (ushort)signedValue;
            return true;
        }

        value = 0;
        return false;
    }

    /// <summary>
    /// Writes a UTF-8 encoded string prefixed with a two-byte Big-Endian length (OASIS section 1.5.3).
    /// </summary>
    public static int WriteString(IBufferWriter<byte> writer, string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Contains('\0'))
        {
            throw new MqttProtocolException("[MQTT-1.5.3-2] UTF-8 strings must not contain the null character U+0000.");
        }

        int byteCount = Utf8Encoding.GetByteCount(value);
        if (byteCount > ushort.MaxValue)
        {
            throw new MqttProtocolException(
                $"String byte length {byteCount} exceeds maximum allowed {ushort.MaxValue}.");
        }

        WriteUInt16(writer, (ushort)byteCount);
        Span<byte> span = writer.GetSpan(byteCount);
        int written = Utf8Encoding.GetBytes(value, span);
        writer.Advance(written);

        return 2 + written;
    }

    /// <summary>
    /// Attempts to read a UTF-8 encoded string prefixed with a two-byte Big-Endian length (OASIS section 1.5.3).
    /// </summary>
    public static bool TryReadString(ref SequenceReader<byte> reader, out string? value)
    {
        if (!TryReadUInt16(ref reader, out ushort length))
        {
            reader.Rewind(reader.Consumed - reader.Consumed); // keep initial
            value = null;
            return false;
        }

        if (reader.Remaining < length)
        {
            reader.Rewind(2);
            value = null;
            return false;
        }

        ReadOnlySequence<byte> stringSequence = reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);

        try
        {
            if (stringSequence.IsSingleSegment)
            {
                value = Utf8Encoding.GetString(stringSequence.FirstSpan);
            }
            else
            {
                // Multi-segment sequence
                byte[] rented = ArrayPool<byte>.Shared.Rent(length);
                try
                {
                    stringSequence.CopyTo(rented);
                    value = Utf8Encoding.GetString(rented.AsSpan(0, length));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }

            if (value.Contains('\0'))
            {
                throw new MqttProtocolException(
                    "[MQTT-1.5.3-2] UTF-8 strings must not contain the null character U+0000.");
            }

            return true;
        }
        catch (DecoderFallbackException ex)
        {
            throw new MqttProtocolException("[MQTT-1.5.3-1] String contains malformed UTF-8 byte sequences.", ex);
        }
    }
}