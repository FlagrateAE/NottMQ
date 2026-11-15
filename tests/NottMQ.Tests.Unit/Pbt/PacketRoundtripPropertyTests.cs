using System.Buffers;
using FsCheck;
using FsCheck.Xunit;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Packets;

namespace NottMQ.Tests.Unit.Pbt;

public class PacketRoundtripPropertyTests
{
    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool PublishPacket_Roundtrip_Invariant_Holds(PublishPacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodePublish(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is PublishPacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool ConnectPacket_Roundtrip_Invariant_Holds(ConnectPacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodeConnect(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is ConnectPacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool ConnAckPacket_Roundtrip_Invariant_Holds(ConnAckPacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodeConnAck(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is ConnAckPacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool SubscribePacket_Roundtrip_Invariant_Holds(SubscribePacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodeSubscribe(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is SubscribePacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool SubAckPacket_Roundtrip_Invariant_Holds(SubAckPacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodeSubAck(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is SubAckPacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool UnsubscribePacket_Roundtrip_Invariant_Holds(UnsubscribePacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.EncodeUnsubscribe(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               decodedPacket is UnsubscribePacket decoded &&
               originalPacket.Equals(decoded);
    }

    [Property(Arbitrary = [typeof(MqttPacketGenerators)])]
    public bool AllPackets_Roundtrip_Invariant_Holds(MqttPacket originalPacket)
    {
        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.Encode(originalPacket, buffer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        bool result = MqttDecoder.TryDecode(ref sequence, out var decodedPacket);

        return result &&
               sequence.Length == 0 &&
               originalPacket.Equals(decodedPacket);
    }
}
