using System.Buffers;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Codecs;

/// <summary>
/// Encodes strongly typed MQTT control packets into their binary wire format.
/// </summary>
public static class MqttEncoder
{
    [ThreadStatic]
    private static ArrayBufferWriter<byte>? _cachedBodyWriter;

    private static ArrayBufferWriter<byte> GetOrCreateBodyWriter()
    {
        var writer = _cachedBodyWriter;
        if (writer == null)
        {
            writer = new ArrayBufferWriter<byte>(1024);
            _cachedBodyWriter = writer;
        }
        else
        {
            writer.Clear();
        }
        return writer;
    }

    /// <summary>
    /// Encodes any supported MQTT packet into the target <see cref="IBufferWriter{Byte}"/>.
    /// </summary>
    public static void Encode(MqttPacket packet, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ArgumentNullException.ThrowIfNull(writer);

        switch (packet)
        {
            case ConnectPacket connect:
                EncodeConnect(connect, writer);
                break;
            case ConnAckPacket connAck:
                EncodeConnAck(connAck, writer);
                break;
            case PublishPacket publish:
                EncodePublish(publish, writer);
                break;
            case PubAckPacket pubAck:
                EncodePubAck(pubAck, writer);
                break;
            case SubscribePacket subscribe:
                EncodeSubscribe(subscribe, writer);
                break;
            case SubAckPacket subAck:
                EncodeSubAck(subAck, writer);
                break;
            case UnsubscribePacket unsubscribe:
                EncodeUnsubscribe(unsubscribe, writer);
                break;
            case UnsubAckPacket unsubAck:
                EncodeUnsubAck(unsubAck, writer);
                break;
            case PingReqPacket:
                EncodeHeaderOnly(PacketType.PingReq, 0, writer);
                break;
            case PingRespPacket:
                EncodeHeaderOnly(PacketType.PingResp, 0, writer);
                break;
            case DisconnectPacket:
                EncodeHeaderOnly(PacketType.Disconnect, 0, writer);
                break;
            default:
                throw new MqttProtocolException($"Unsupported packet type for encoding: {packet.GetType().Name}");
        }
    }

    public static void EncodeConnect(ConnectPacket packet, IBufferWriter<byte> writer)
    {
        var bodyWriter = GetOrCreateBodyWriter();

        // Variable Header
        MqttPrimitives.WriteString(bodyWriter, packet.ProtocolName);
        Span<byte> protoLevel = bodyWriter.GetSpan(1);
        protoLevel[0] = packet.ProtocolLevel;
        bodyWriter.Advance(1);

        // Connect Flags
        byte flags = 0;
        if (packet.CleanSession) flags |= 0x02;
        if (packet.HasWill)
        {
            flags |= 0x04;
            flags |= (byte)(((byte)packet.WillQoS & 0x03) << 3);
            if (packet.WillRetain) flags |= 0x20;
        }
        if (packet.Password != null) flags |= 0x40;
        if (packet.Username != null) flags |= 0x80;

        Span<byte> flagSpan = bodyWriter.GetSpan(1);
        flagSpan[0] = flags;
        bodyWriter.Advance(1);

        MqttPrimitives.WriteUInt16(bodyWriter, packet.KeepAliveSeconds);

        // Payload
        MqttPrimitives.WriteString(bodyWriter, packet.ClientId);
        if (packet.HasWill)
        {
            MqttPrimitives.WriteString(bodyWriter, packet.WillTopic ?? string.Empty);
            ReadOnlyMemory<byte> willMsg = packet.WillMessage ?? ReadOnlyMemory<byte>.Empty;
            MqttPrimitives.WriteUInt16(bodyWriter, (ushort)willMsg.Length);
            bodyWriter.Write(willMsg.Span);
        }
        if (packet.Username != null)
        {
            MqttPrimitives.WriteString(bodyWriter, packet.Username);
        }
        if (packet.Password != null)
        {
            MqttPrimitives.WriteString(bodyWriter, packet.Password);
        }

        WritePacket(PacketType.Connect, 0x00, bodyWriter.WrittenSpan, writer);
    }

    public static void EncodeConnAck(ConnAckPacket packet, IBufferWriter<byte> writer)
    {
        Span<byte> body = stackalloc byte[2];
        body[0] = packet.SessionPresent ? (byte)0x01 : (byte)0x00;
        body[1] = (byte)packet.ReturnCode;

        WritePacket(PacketType.ConnAck, 0x00, body, writer);
    }

    public static void EncodePublish(PublishPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.QualityOfService > MqttQoS.AtLeastOnce)
        {
            throw new MqttProtocolException($"QoS {(int)packet.QualityOfService} is not supported.");
        }

        if (packet.QualityOfService == MqttQoS.AtMostOnce && packet.PacketId.HasValue)
        {
            throw new MqttProtocolException("[MQTT-2.3.1-5] Packet Identifier MUST NOT be present in QoS 0 PUBLISH packet.");
        }

        if (packet.QualityOfService > MqttQoS.AtMostOnce && !packet.PacketId.HasValue)
        {
            throw new MqttProtocolException("[MQTT-2.3.1-1] Packet Identifier MUST be present in QoS > 0 PUBLISH packet.");
        }

        byte flags = 0;
        if (packet.Duplicate) flags |= 0x08;
        flags |= (byte)(((byte)packet.QualityOfService & 0x03) << 1);
        if (packet.Retain) flags |= 0x01;

        var bodyWriter = GetOrCreateBodyWriter();

        // Variable Header: Topic Name
        MqttPrimitives.WriteString(bodyWriter, packet.Topic);

        // Packet Identifier (if QoS > 0)
        if (packet.QualityOfService > MqttQoS.AtMostOnce)
        {
            MqttPrimitives.WriteUInt16(bodyWriter, packet.PacketId!.Value);
        }

        // Payload
        if (!packet.Payload.IsEmpty)
        {
            bodyWriter.Write(packet.Payload.Span);
        }

        WritePacket(PacketType.Publish, flags, bodyWriter.WrittenSpan, writer);
    }

    public static void EncodePubAck(PubAckPacket packet, IBufferWriter<byte> writer)
    {
        Span<byte> body = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(body, packet.PacketId);
        WritePacket(PacketType.PubAck, 0x00, body, writer);
    }

    public static void EncodeSubscribe(SubscribePacket packet, IBufferWriter<byte> writer)
    {
        if (packet.Subscriptions == null || packet.Subscriptions.Count == 0)
        {
            throw new MqttProtocolException("[MQTT-3.8.3-3] The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair.");
        }

        var bodyWriter = GetOrCreateBodyWriter();
        MqttPrimitives.WriteUInt16(bodyWriter, packet.PacketId);

        foreach (var sub in packet.Subscriptions)
        {
            MqttPrimitives.WriteString(bodyWriter, sub.TopicFilter);
            Span<byte> qosSpan = bodyWriter.GetSpan(1);
            qosSpan[0] = (byte)sub.RequestedQoS;
            bodyWriter.Advance(1);
        }

        // [MQTT-3.8.1-1] Reserved flags MUST be 0010 (0x02)
        WritePacket(PacketType.Subscribe, 0x02, bodyWriter.WrittenSpan, writer);
    }

    public static void EncodeSubAck(SubAckPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.ReturnCodes == null || packet.ReturnCodes.Count == 0)
        {
            throw new MqttProtocolException("SUBACK must contain at least one return code.");
        }

        var bodyWriter = GetOrCreateBodyWriter();
        MqttPrimitives.WriteUInt16(bodyWriter, packet.PacketId);

        foreach (var code in packet.ReturnCodes)
        {
            Span<byte> codeSpan = bodyWriter.GetSpan(1);
            codeSpan[0] = (byte)code;
            bodyWriter.Advance(1);
        }

        WritePacket(PacketType.SubAck, 0x00, bodyWriter.WrittenSpan, writer);
    }

    public static void EncodeUnsubscribe(UnsubscribePacket packet, IBufferWriter<byte> writer)
    {
        if (packet.Topics == null || packet.Topics.Count == 0)
        {
            throw new MqttProtocolException("[MQTT-3.10.3-2] The payload of an UNSUBSCRIBE packet MUST contain at least one Topic Filter.");
        }

        var bodyWriter = GetOrCreateBodyWriter();
        MqttPrimitives.WriteUInt16(bodyWriter, packet.PacketId);

        foreach (var topic in packet.Topics)
        {
            MqttPrimitives.WriteString(bodyWriter, topic);
        }

        // [MQTT-3.10.1-1] Reserved flags MUST be 0010 (0x02)
        WritePacket(PacketType.Unsubscribe, 0x02, bodyWriter.WrittenSpan, writer);
    }

    public static void EncodeUnsubAck(UnsubAckPacket packet, IBufferWriter<byte> writer)
    {
        Span<byte> body = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(body, packet.PacketId);
        WritePacket(PacketType.UnsubAck, 0x00, body, writer);
    }

    private static void EncodeHeaderOnly(PacketType type, byte flags, IBufferWriter<byte> writer)
    {
        WritePacket(type, flags, ReadOnlySpan<byte>.Empty, writer);
    }

    private static void WritePacket(PacketType type, byte flags, ReadOnlySpan<byte> body, IBufferWriter<byte> writer)
    {
        byte firstByte = (byte)(((byte)type << 4) | (flags & 0x0F));
        Span<byte> headerByteSpan = writer.GetSpan(1);
        headerByteSpan[0] = firstByte;
        writer.Advance(1);

        MqttRemainingLength.Write(writer, body.Length);

        if (!body.IsEmpty)
        {
            writer.Write(body);
        }
    }
}
