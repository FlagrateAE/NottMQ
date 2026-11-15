using System.Buffers;
using FsCheck;
using FsCheck.Xunit;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Packets;

namespace NottMQ.Tests.Unit.Pbt;

public class StreamFragmentationPropertyTests
{
    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool MultiSegment_Sequence_Decodes_Identically(MqttPacket packet)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.Encode(packet, buffer);
        byte[] bytes = buffer.WrittenSpan.ToArray();

        if (bytes.Length == 0) return true;

        // Split into 1-byte, 2-byte, or 3-byte segments to force sequence boundaries across headers & payload
        for (int chunkSize = 1; chunkSize <= Math.Min(bytes.Length, 7); chunkSize++)
        {
            var chunks = new List<ReadOnlyMemory<byte>>();
            for (int offset = 0; offset < bytes.Length; offset += chunkSize)
            {
                int len = Math.Min(chunkSize, bytes.Length - offset);
                chunks.Add(new ReadOnlyMemory<byte>(bytes, offset, len));
            }

            var sequence = SequenceSegment<byte>.Create(chunks);
            bool decoded = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

            if (!decoded || sequence.Length != 0 || !packet.Equals(decodedPacket))
            {
                return false;
            }
        }

        return true;
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool Incremental_Streaming_Reassembly_Holds(MqttPacket packet)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.Encode(packet, buffer);
        byte[] bytes = buffer.WrittenSpan.ToArray();

        // Simulate streaming byte-by-byte into a buffer:
        // Any prefix of length < bytes.Length must return false (NeedMoreData)
        for (int i = 0; i < bytes.Length; i++)
        {
            var prefixSequence = new ReadOnlySequence<byte>(bytes, 0, i);
            bool partialResult = MqttDecoder.TryDecode(ref prefixSequence, out var partialPacket);

            if (partialResult || partialPacket != null)
            {
                // Must not prematurely return true on incomplete packet
                return false;
            }
        }

        // Full buffer must decode cleanly
        var fullSequence = new ReadOnlySequence<byte>(bytes);
        bool success = MqttDecoder.TryDecode(ref fullSequence, out var resultPacket);

        return success && fullSequence.Length == 0 && packet.Equals(resultPacket);
    }
}
