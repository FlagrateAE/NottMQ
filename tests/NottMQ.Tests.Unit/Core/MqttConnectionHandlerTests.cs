using FluentAssertions;
using NottMQ.Core.Configuration;
using NottMQ.Core.Sessions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Unit.Core;

public class MqttConnectionHandlerTests
{
    [Fact]
    public async Task HandlePacket_FirstPacketNotConnect_ClosesWithoutConnAck()
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(PingReqPacket.Instance);

        conn.IsClosed.Should().BeTrue();
        conn.SentPackets.Should().BeEmpty();
        handler.Session.Should().BeNull();
    }

    [Fact]
    public async Task HandlePacket_SecondConnect_ClosesConnection()
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });
        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });

        conn.IsClosed.Should().BeTrue();
        conn.SentPackets.Should().ContainSingle()
            .Which.Should().BeOfType<ConnAckPacket>();
    }

    [Theory]
    [InlineData("MQTT", 3)]
    [InlineData("MQIsdp", 4)]
    public async Task HandlePacket_InvalidProtocol_SendsUnacceptableProtocolVersionAndCloses(
        string protocolName,
        byte protocolLevel)
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket
        {
            ClientId = "client-1",
            ProtocolName = protocolName,
            ProtocolLevel = protocolLevel
        });

        conn.IsClosed.Should().BeTrue();
        conn.SentPackets.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new ConnAckPacket
            {
                SessionPresent = false,
                ReturnCode = ConnectReturnCode.UnacceptableProtocolVersion
            });
    }

    [Fact]
    public async Task HandlePacket_EmptyClientIdCleanSessionFalse_SendsIdentifierRejectedAndCloses()
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket
        {
            ClientId = string.Empty,
            CleanSession = false
        });

        conn.IsClosed.Should().BeTrue();
        conn.SentPackets.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new ConnAckPacket
            {
                SessionPresent = false,
                ReturnCode = ConnectReturnCode.IdentifierRejected
            });
    }

    [Fact]
    public async Task HandlePacket_ValidConnect_SendsConnAckAccepted()
    {
        var conn = new FakeClientConnection();
        var sessions = new SessionManager();
        var handler = new MqttConnectionHandler(conn, sessions, new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });

        conn.IsClosed.Should().BeFalse();
        handler.Session.Should().NotBeNull();
        sessions.ActiveSessionCount.Should().Be(1);
        conn.SentPackets.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new ConnAckPacket
            {
                SessionPresent = false,
                ReturnCode = ConnectReturnCode.Accepted
            });
    }

    [Fact]
    public async Task HandlePacket_PingReq_SendsPingRespAndUpdatesActivity()
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });
        DateTime beforePing = handler.Session!.LastActivityUtc;

        await Task.Delay(15);
        await handler.HandlePacketAsync(PingReqPacket.Instance);

        conn.IsClosed.Should().BeFalse();
        handler.Session.LastActivityUtc.Should().BeOnOrAfter(beforePing);
        conn.SentPackets.Should().HaveCount(2);
        conn.SentPackets[1].Should().BeSameAs(PingRespPacket.Instance);
    }

    [Fact]
    public async Task HandlePacket_Disconnect_MarksCleanDisconnectAndCloses()
    {
        var conn = new FakeClientConnection();
        var handler = new MqttConnectionHandler(conn, new SessionManager(), new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });
        ClientSession session = handler.Session!;

        await handler.HandlePacketAsync(DisconnectPacket.Instance);

        session.IsCleanDisconnect.Should().BeTrue();
        conn.IsClosed.Should().BeTrue();
    }

    [Fact]
    public async Task OnDisconnectedAsync_RemovesSessionFromManager()
    {
        var conn = new FakeClientConnection();
        var sessions = new SessionManager();
        var handler = new MqttConnectionHandler(conn, sessions, new BrokerOptions());

        await handler.HandlePacketAsync(new ConnectPacket { ClientId = "client-1" });
        sessions.ActiveSessionCount.Should().Be(1);

        await handler.OnDisconnectedAsync();
        await handler.OnDisconnectedAsync();

        handler.Session.Should().BeNull();
        sessions.ActiveSessionCount.Should().Be(0);
    }
}
