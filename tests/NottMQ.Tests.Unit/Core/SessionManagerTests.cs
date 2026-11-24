using FluentAssertions;
using NottMQ.Core.Sessions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Tests.Unit.Core;

public class SessionManagerTests
{
    [Fact]
    public async Task TryRegisterSession_EmptyClientId_CleanSessionTrue_GeneratesUniqueClientId()
    {
        var manager = new SessionManager();
        var conn1 = new FakeClientConnection("c1");
        var conn2 = new FakeClientConnection("c2");

        var result1 = await manager.TryRegisterSessionAsync(
            new ConnectPacket { ClientId = string.Empty, CleanSession = true },
            conn1);
        var result2 = await manager.TryRegisterSessionAsync(
            new ConnectPacket { ClientId = string.Empty, CleanSession = true },
            conn2);

        result1.Succeeded.Should().BeTrue();
        result1.ReturnCode.Should().Be(ConnectReturnCode.Accepted);
        result1.Session.Should().NotBeNull();
        result1.Session!.ClientId.Should().NotBeNullOrWhiteSpace();

        result2.Succeeded.Should().BeTrue();
        result2.Session.Should().NotBeNull();
        result2.Session!.ClientId.Should().NotBe(result1.Session.ClientId);
        manager.ActiveSessionCount.Should().Be(2);
    }

    [Fact]
    public async Task TryRegisterSession_EmptyClientId_CleanSessionFalse_ReturnsIdentifierRejected()
    {
        var manager = new SessionManager();
        var conn = new FakeClientConnection("c1");

        var result = await manager.TryRegisterSessionAsync(
            new ConnectPacket { ClientId = string.Empty, CleanSession = false },
            conn);

        result.Succeeded.Should().BeFalse();
        result.ReturnCode.Should().Be(ConnectReturnCode.IdentifierRejected);
        result.Session.Should().BeNull();
        manager.ActiveSessionCount.Should().Be(0);
    }

    [Fact]
    public async Task TryRegisterSession_DuplicateClientId_PerformsTakeoverAndGuardsRemoveSession()
    {
        var manager = new SessionManager();
        var oldConn = new FakeClientConnection("old");
        var newConn = new FakeClientConnection("new");

        var first = await manager.TryRegisterSessionAsync(
            new ConnectPacket { ClientId = "device-1", CleanSession = true },
            oldConn);

        var second = await manager.TryRegisterSessionAsync(
            new ConnectPacket { ClientId = "device-1", CleanSession = true },
            newConn);

        oldConn.IsClosed.Should().BeTrue();
        oldConn.CloseReason.Should().Be("Session takeover");

        second.Succeeded.Should().BeTrue();
        newConn.IsClosed.Should().BeFalse();
        manager.ActiveSessionCount.Should().Be(1);
        manager.TryGetSession("device-1", out ClientSession? current).Should().BeTrue();
        current.Should().BeSameAs(second.Session);

        // Removing the stale session reference must not remove the new active session.
        manager.RemoveSession("device-1", first.Session!).Should().BeFalse();
        manager.ActiveSessionCount.Should().Be(1);

        // Removing the current session reference succeeds.
        manager.RemoveSession("device-1", second.Session!).Should().BeTrue();
        manager.ActiveSessionCount.Should().Be(0);
    }

    [Fact]
    public void ClientSession_HasKeepAliveExpired_RespectsZeroAndMultiplier()
    {
        var conn = new FakeClientConnection();
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var zeroKeepAlive = new ClientSession("c0", true, 0, conn, start);
        zeroKeepAlive.HasKeepAliveExpired(start.AddHours(10), 1.5).Should().BeFalse();

        var session = new ClientSession("c1", true, 10, conn, start);
        session.HasKeepAliveExpired(start.AddSeconds(15), 1.5).Should().BeFalse();
        session.HasKeepAliveExpired(start.AddSeconds(15.1), 1.5).Should().BeTrue();
    }
}
