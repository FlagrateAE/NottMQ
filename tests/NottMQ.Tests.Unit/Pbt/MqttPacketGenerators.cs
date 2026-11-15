using FsCheck;
using FsCheck.Fluent;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Unit.Pbt;

public static class MqttPacketGenerators
{
    private static readonly string[] TopicSegments =
    [
        "home", "office", "sensor", "temp", "humidity", "light",
        "power", "device1", "device2", "status", "data", "metrics"
    ];

    private static readonly Gen<string> ValidTopicGen =
        from seg1 in Gen.Elements(TopicSegments)
        from seg2 in Gen.Elements(TopicSegments)
        select $"{seg1}/{seg2}";

    private static readonly Gen<string> SafeStringGen =
        from id in Gen.Choose(1, 10000)
        from prefix in Gen.Elements("client_", "node_", "device_", "user_")
        select $"{prefix}{id}";

    private static readonly Gen<bool> BoolGen = Gen.Elements(true, false);
    private static readonly Gen<byte> ByteGen = Gen.Choose(0, 255).Select(x => (byte)x);

    public static Arbitrary<PublishPacket> PublishPackets()
    {
        var gen = from topic in ValidTopicGen
                  from qos in Gen.Elements(MqttQoS.AtMostOnce, MqttQoS.AtLeastOnce)
                  from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  from dup in BoolGen
                  from retain in BoolGen
                  from payloadLength in Gen.Choose(0, 64)
                  from payloadBytes in Gen.ListOf(ByteGen, payloadLength)
                  select new PublishPacket
                  {
                      Topic = topic,
                      QualityOfService = qos,
                      PacketId = qos == MqttQoS.AtMostOnce ? null : packetId,
                      Duplicate = qos == MqttQoS.AtMostOnce ? false : dup,
                      Retain = retain,
                      Payload = payloadBytes.ToArray()
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<ConnectPacket> ConnectPackets()
    {
        var gen = from clientId in SafeStringGen
                  from cleanSession in BoolGen
                  from keepAlive in Gen.Choose(0, 3600).Select(x => (ushort)x)
                  from hasAuth in BoolGen
                  from username in SafeStringGen
                  from password in SafeStringGen
                  select new ConnectPacket
                  {
                      ClientId = clientId,
                      CleanSession = cleanSession,
                      KeepAliveSeconds = keepAlive,
                      Username = hasAuth ? username : null,
                      Password = hasAuth ? password : null,
                      HasWill = false
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<ConnAckPacket> ConnAckPackets()
    {
        var gen = from sessionPresent in BoolGen
                  from returnCode in Gen.Elements(
                      ConnectReturnCode.Accepted,
                      ConnectReturnCode.UnacceptableProtocolVersion,
                      ConnectReturnCode.IdentifierRejected,
                      ConnectReturnCode.ServerUnavailable,
                      ConnectReturnCode.BadUsernameOrPassword,
                      ConnectReturnCode.NotAuthorized)
                  select new ConnAckPacket
                  {
                      SessionPresent = sessionPresent,
                      ReturnCode = returnCode
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<PubAckPacket> PubAckPackets()
    {
        var gen = from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  select new PubAckPacket { PacketId = packetId };

        return Arb.From(gen);
    }

    public static Arbitrary<SubscribePacket> SubscribePackets()
    {
        var topicSubGen = from topic in ValidTopicGen
                          from qos in Gen.Elements(MqttQoS.AtMostOnce, MqttQoS.AtLeastOnce)
                          select new TopicSubscription(topic, qos);

        var gen = from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  from count in Gen.Choose(1, 4)
                  from subs in Gen.ListOf(topicSubGen, count)
                  select new SubscribePacket
                  {
                      PacketId = packetId,
                      Subscriptions = subs.ToList()
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<SubAckPacket> SubAckPackets()
    {
        var returnCodeGen = Gen.Elements(
            SubscribeReturnCode.SuccessQoS0,
            SubscribeReturnCode.SuccessQoS1,
            SubscribeReturnCode.Failure);

        var gen = from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  from count in Gen.Choose(1, 4)
                  from codes in Gen.ListOf(returnCodeGen, count)
                  select new SubAckPacket
                  {
                      PacketId = packetId,
                      ReturnCodes = codes.ToList()
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<UnsubscribePacket> UnsubscribePackets()
    {
        var gen = from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  from count in Gen.Choose(1, 4)
                  from topics in Gen.ListOf(ValidTopicGen, count)
                  select new UnsubscribePacket
                  {
                      PacketId = packetId,
                      Topics = topics.ToList()
                  };

        return Arb.From(gen);
    }

    public static Arbitrary<UnsubAckPacket> UnsubAckPackets()
    {
        var gen = from packetId in Gen.Choose(1, (int)ushort.MaxValue).Select(x => (ushort)x)
                  select new UnsubAckPacket { PacketId = packetId };

        return Arb.From(gen);
    }

    public static Arbitrary<MqttPacket> MqttPackets()
    {
        var gen = Gen.OneOf(
            PublishPackets().Generator.Select(p => (MqttPacket)p),
            ConnectPackets().Generator.Select(p => (MqttPacket)p),
            ConnAckPackets().Generator.Select(p => (MqttPacket)p),
            PubAckPackets().Generator.Select(p => (MqttPacket)p),
            SubscribePackets().Generator.Select(p => (MqttPacket)p),
            SubAckPackets().Generator.Select(p => (MqttPacket)p),
            UnsubscribePackets().Generator.Select(p => (MqttPacket)p),
            UnsubAckPackets().Generator.Select(p => (MqttPacket)p),
            Gen.Constant<MqttPacket>(PingReqPacket.Instance),
            Gen.Constant<MqttPacket>(PingRespPacket.Instance),
            Gen.Constant<MqttPacket>(DisconnectPacket.Instance)
        );

        return Arb.From(gen);
    }
}
