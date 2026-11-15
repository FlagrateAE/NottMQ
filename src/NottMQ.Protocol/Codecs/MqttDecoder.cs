using System.Buffers;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Codecs;

/// <summary>
/// Decodes MQTT control packets from continuous byte sequences (<see cref="ReadOnlySequence{Byte}"/>).
/// Supports handling incomplete network frames gracefully.
/// </summary>
public static class MqttDecoder
{
    /// <summary>
    /// Attempts to decode a single MQTT packet from the provided sequence buffer,
    /// advancing the buffer if successful.
    /// </summary>
    public static bool TryDecode(ref ReadOnlySequence<byte> buffer, out MqttPacket? packet)
    {
        if (TryDecode(buffer, out packet, out SequencePosition consumed, out _))
        {
            buffer = buffer.Slice(consumed);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Attempts to decode a single MQTT packet from the buffer sequence without modifying the buffer reference directly.
    /// Returns consumed and examined positions for compatibility with <c>System.IO.Pipelines</c>.
    /// </summary>
    public static bool TryDecode(
        ReadOnlySequence<byte> buffer,
        out MqttPacket? packet,
        out SequencePosition consumed,
        out SequencePosition examined)
    {
        packet = null;
        consumed = buffer.Start;
        examined = buffer.End;

        if (buffer.Length < 2)
        {
            return false;
        }

        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryRead(out byte headerByte))
        {
            return false;
        }

        var packetType = (PacketType)(headerByte >> 4);
        byte flags = (byte)(headerByte & 0x0F);

        if (!MqttRemainingLength.TryRead(ref reader, out int remainingLength, out int lengthBytes))
        {
            return false;
        }

        int headerLength = 1 + lengthBytes;
        long totalLength = headerLength + remainingLength;

        if (buffer.Length < totalLength)
        {
            return false;
        }

        // We have the complete packet
        ReadOnlySequence<byte> packetBody = buffer.Slice(headerLength, remainingLength);
        SequencePosition packetEnd = buffer.GetPosition(totalLength);
        consumed = packetEnd;
        examined = packetEnd;

        ValidateFixedHeaderFlags(packetType, flags);

        var bodyReader = new SequenceReader<byte>(packetBody);
        packet = DecodePacket(packetType, flags, ref bodyReader, packetBody);
        return true;
    }

    private static void ValidateFixedHeaderFlags(PacketType type, byte flags)
    {
        switch (type)
        {
            case PacketType.Connect:
            case PacketType.ConnAck:
            case PacketType.PubAck:
            case PacketType.SubAck:
            case PacketType.UnsubAck:
            case PacketType.PingReq:
            case PacketType.PingResp:
            case PacketType.Disconnect:
                if (flags != 0)
                {
                    throw new MqttProtocolException($"[MQTT-2.2.2-2] Flags for packet type {type} MUST be 0, found: {flags}.");
                }
                break;

            case PacketType.Subscribe:
                if (flags != 0x02)
                {
                    throw new MqttProtocolException($"[MQTT-3.8.1-1] Reserved flags for SUBSCRIBE MUST be 0010 (0x02), found: {flags}.");
                }
                break;

            case PacketType.Unsubscribe:
                if (flags != 0x02)
                {
                    throw new MqttProtocolException($"[MQTT-3.10.1-1] Reserved flags for UNSUBSCRIBE MUST be 0010 (0x02), found: {flags}.");
                }
                break;

            case PacketType.Publish:
                byte qosBits = (byte)((flags >> 1) & 0x03);
                if (qosBits == 3)
                {
                    throw new MqttProtocolException("[MQTT-3.3.1-4] PUBLISH QoS 3 is reserved and MUST NOT be used.");
                }
                if (qosBits == 0 && (flags & 0x08) != 0)
                {
                    throw new MqttProtocolException("[MQTT-3.3.1-2] The DUP flag MUST be set to 0 for all QoS 0 messages.");
                }
                break;

            default:
                throw new MqttProtocolException($"Invalid or unsupported packet type: {type}.");
        }
    }

    private static MqttPacket DecodePacket(
        PacketType packetType,
        byte flags,
        ref SequenceReader<byte> reader,
        ReadOnlySequence<byte> bodySequence)
    {
        return packetType switch
        {
            PacketType.Connect => DecodeConnect(ref reader),
            PacketType.ConnAck => DecodeConnAck(ref reader),
            PacketType.Publish => DecodePublish(flags, ref reader, bodySequence),
            PacketType.PubAck => DecodePubAck(ref reader),
            PacketType.Subscribe => DecodeSubscribe(ref reader),
            PacketType.SubAck => DecodeSubAck(ref reader),
            PacketType.Unsubscribe => DecodeUnsubscribe(ref reader),
            PacketType.UnsubAck => DecodeUnsubAck(ref reader),
            PacketType.PingReq => PingReqPacket.Instance,
            PacketType.PingResp => PingRespPacket.Instance,
            PacketType.Disconnect => DisconnectPacket.Instance,
            _ => throw new MqttProtocolException($"Unsupported packet type: {packetType}")
        };
    }

    private static ConnectPacket DecodeConnect(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadString(ref reader, out string? protocolName) || protocolName != "MQTT")
        {
            throw new MqttProtocolException($"[MQTT-3.1.2-1] Invalid protocol name '{protocolName}', expected 'MQTT'.");
        }

        if (!reader.TryRead(out byte protocolLevel))
        {
            throw new MqttProtocolException("Unexpected EOF reading Protocol Level in CONNECT.");
        }

        if (protocolLevel != 4)
        {
            throw new MqttProtocolException($"[MQTT-3.1.2-2] Unsupported protocol level '{protocolLevel}', expected 4 (v3.1.1).");
        }

        if (!reader.TryRead(out byte connectFlags))
        {
            throw new MqttProtocolException("Unexpected EOF reading Connect Flags.");
        }

        // [MQTT-3.1.2-3] The Server MUST validate that the reserved flag in the CONNECT packet is set to 0.
        if ((connectFlags & 0x01) != 0)
        {
            throw new MqttProtocolException("[MQTT-3.1.2-3] The reserved flag in CONNECT packet MUST be 0.");
        }

        bool cleanSession = (connectFlags & 0x02) != 0;
        bool willFlag = (connectFlags & 0x04) != 0;
        var willQoS = (MqttQoS)((connectFlags >> 3) & 0x03);
        bool willRetain = (connectFlags & 0x20) != 0;
        bool passwordFlag = (connectFlags & 0x40) != 0;
        bool usernameFlag = (connectFlags & 0x80) != 0;

        if (!willFlag && (willQoS != MqttQoS.AtMostOnce || willRetain))
        {
            throw new MqttProtocolException("[MQTT-3.1.2-11] If Will Flag is 0, Will QoS and Will Retain MUST be 0.");
        }

        if (willQoS == (MqttQoS)3)
        {
            throw new MqttProtocolException("[MQTT-3.1.2-14] Will QoS MUST NOT be 3.");
        }

        if (!usernameFlag && passwordFlag)
        {
            throw new MqttProtocolException("[MQTT-3.1.2-22] If the User Name Flag is 0, Password Flag MUST be 0.");
        }

        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort keepAlive))
        {
            throw new MqttProtocolException("Unexpected EOF reading KeepAlive.");
        }

        // Payload
        if (!MqttPrimitives.TryReadString(ref reader, out string? clientId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Client Identifier.");
        }

        string? willTopic = null;
        ReadOnlyMemory<byte>? willMessage = null;
        if (willFlag)
        {
            if (!MqttPrimitives.TryReadString(ref reader, out willTopic))
            {
                throw new MqttProtocolException("Unexpected EOF reading Will Topic.");
            }

            if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort willMsgLength))
            {
                throw new MqttProtocolException("Unexpected EOF reading Will Message length.");
            }

            if (reader.Remaining < willMsgLength)
            {
                throw new MqttProtocolException("Unexpected EOF reading Will Message payload.");
            }

            var willSeq = reader.Sequence.Slice(reader.Position, willMsgLength);
            reader.Advance(willMsgLength);
            willMessage = willSeq.ToArray();
        }

        string? username = null;
        if (usernameFlag)
        {
            if (!MqttPrimitives.TryReadString(ref reader, out username))
            {
                throw new MqttProtocolException("Unexpected EOF reading Username.");
            }
        }

        string? password = null;
        if (passwordFlag)
        {
            if (!MqttPrimitives.TryReadString(ref reader, out password))
            {
                throw new MqttProtocolException("Unexpected EOF reading Password.");
            }
        }

        return new ConnectPacket
        {
            ProtocolName = protocolName,
            ProtocolLevel = protocolLevel,
            CleanSession = cleanSession,
            KeepAliveSeconds = keepAlive,
            ClientId = clientId!,
            HasWill = willFlag,
            WillQoS = willQoS,
            WillRetain = willRetain,
            WillTopic = willTopic,
            WillMessage = willMessage,
            Username = username,
            Password = password
        };
    }

    private static ConnAckPacket DecodeConnAck(ref SequenceReader<byte> reader)
    {
        if (!reader.TryRead(out byte flagsByte))
        {
            throw new MqttProtocolException("Unexpected EOF reading CONNACK flags.");
        }

        if ((flagsByte & 0xFE) != 0)
        {
            throw new MqttProtocolException("[MQTT-3.2.2-2] The 7 reserved bits in CONNACK flags MUST be 0.");
        }

        bool sessionPresent = (flagsByte & 0x01) != 0;

        if (!reader.TryRead(out byte returnCodeByte))
        {
            throw new MqttProtocolException("Unexpected EOF reading CONNACK return code.");
        }

        return new ConnAckPacket
        {
            SessionPresent = sessionPresent,
            ReturnCode = (ConnectReturnCode)returnCodeByte
        };
    }

    private static PublishPacket DecodePublish(byte flags, ref SequenceReader<byte> reader, ReadOnlySequence<byte> bodySequence)
    {
        bool dup = (flags & 0x08) != 0;
        var qos = (MqttQoS)((flags >> 1) & 0x03);
        bool retain = (flags & 0x01) != 0;

        if (!MqttPrimitives.TryReadString(ref reader, out string? topic) || topic == null)
        {
            throw new MqttProtocolException("Unexpected EOF reading Topic Name in PUBLISH.");
        }

        // [MQTT-4.7.1-1] Topic Names MUST NOT contain wildcards '+' or '#'
        if (topic.Contains('+') || topic.Contains('#'))
        {
            throw new MqttProtocolException($"[MQTT-4.7.1-1] The Topic Name in the PUBLISH packet MUST NOT contain wildcard characters: {topic}");
        }

        ushort? packetId = null;
        if (qos > MqttQoS.AtMostOnce)
        {
            if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort id))
            {
                throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in PUBLISH.");
            }
            packetId = id;
        }

        // Remaining bytes form the Payload
        ReadOnlySequence<byte> payloadSequence = bodySequence.Slice(reader.Position);
        ReadOnlyMemory<byte> payload = payloadSequence.Length > 0 ? payloadSequence.ToArray() : ReadOnlyMemory<byte>.Empty;
        reader.Advance(payloadSequence.Length);

        return new PublishPacket
        {
            Topic = topic,
            PacketId = packetId,
            QualityOfService = qos,
            Duplicate = dup,
            Retain = retain,
            Payload = payload
        };
    }

    private static PubAckPacket DecodePubAck(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort packetId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in PUBACK.");
        }

        return new PubAckPacket { PacketId = packetId };
    }

    private static SubscribePacket DecodeSubscribe(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort packetId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in SUBSCRIBE.");
        }

        var subscriptions = new List<TopicSubscription>();

        while (reader.Remaining > 0)
        {
            if (!MqttPrimitives.TryReadString(ref reader, out string? topicFilter) || string.IsNullOrEmpty(topicFilter))
            {
                throw new MqttProtocolException("Invalid or empty topic filter in SUBSCRIBE.");
            }

            if (!reader.TryRead(out byte qosByte))
            {
                throw new MqttProtocolException("Unexpected EOF reading requested QoS in SUBSCRIBE.");
            }

            if ((qosByte & 0xFC) != 0)
            {
                throw new MqttProtocolException("[MQTT-3.8.3-4] Reserved bits in SUBSCRIBE requested QoS MUST be 0.");
            }

            var qos = (MqttQoS)qosByte;
            if (qos > MqttQoS.ExactlyOnce)
            {
                throw new MqttProtocolException($"Invalid QoS {qosByte} in SUBSCRIBE.");
            }

            subscriptions.Add(new TopicSubscription(topicFilter, qos));
        }

        if (subscriptions.Count == 0)
        {
            throw new MqttProtocolException("[MQTT-3.8.3-3] SUBSCRIBE must contain at least one topic filter.");
        }

        return new SubscribePacket
        {
            PacketId = packetId,
            Subscriptions = subscriptions
        };
    }

    private static SubAckPacket DecodeSubAck(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort packetId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in SUBACK.");
        }

        var returnCodes = new List<SubscribeReturnCode>();

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte codeByte))
            {
                break;
            }

            if (codeByte != 0x00 && codeByte != 0x01 && codeByte != 0x02 && codeByte != 0x80)
            {
                throw new MqttProtocolException($"[MQTT-3.9.3-2] Invalid SUBACK return code: 0x{codeByte:X2}.");
            }

            returnCodes.Add((SubscribeReturnCode)codeByte);
        }

        if (returnCodes.Count == 0)
        {
            throw new MqttProtocolException("SUBACK must contain at least one return code.");
        }

        return new SubAckPacket
        {
            PacketId = packetId,
            ReturnCodes = returnCodes
        };
    }

    private static UnsubscribePacket DecodeUnsubscribe(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort packetId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in UNSUBSCRIBE.");
        }

        var topics = new List<string>();

        while (reader.Remaining > 0)
        {
            if (!MqttPrimitives.TryReadString(ref reader, out string? topic) || string.IsNullOrEmpty(topic))
            {
                throw new MqttProtocolException("Invalid or empty topic filter in UNSUBSCRIBE.");
            }

            topics.Add(topic);
        }

        if (topics.Count == 0)
        {
            throw new MqttProtocolException("[MQTT-3.10.3-2] UNSUBSCRIBE must contain at least one topic filter.");
        }

        return new UnsubscribePacket
        {
            PacketId = packetId,
            Topics = topics
        };
    }

    private static UnsubAckPacket DecodeUnsubAck(ref SequenceReader<byte> reader)
    {
        if (!MqttPrimitives.TryReadUInt16(ref reader, out ushort packetId))
        {
            throw new MqttProtocolException("Unexpected EOF reading Packet Identifier in UNSUBACK.");
        }

        return new UnsubAckPacket { PacketId = packetId };
    }
}
