using NottMQ.Core.Abstractions;
using NottMQ.Core.Configuration;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Core.Sessions;

/// <summary>
/// Handles the MQTT v3.1.1 connection lifecycle and control packets for a single client connection.
/// </summary>
public sealed class MqttConnectionHandler
{
    private const string SupportedProtocolName = "MQTT";
    private const byte SupportedProtocolLevel = 4;

    private readonly IClientConnection _connection;
    private readonly SessionManager _sessionManager;
    private readonly BrokerOptions _options;

    public MqttConnectionHandler(
        IClientConnection connection,
        SessionManager sessionManager,
        BrokerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(sessionManager);

        _connection = connection;
        _sessionManager = sessionManager;
        _options = options ?? new BrokerOptions();
    }

    public ClientSession? Session { get; private set; }

    public async ValueTask HandlePacketAsync(MqttPacket packet, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(packet);

        if (Session is null)
        {
            await HandleConnectHandshakeAsync(packet, cancellationToken).ConfigureAwait(false);
            return;
        }

        Session.Touch();

        switch (packet)
        {
            case ConnectPacket:
                // [MQTT-3.1.0-2] A Client can only send the CONNECT Packet once over a Network Connection.
                await _connection.CloseAsync("Duplicate CONNECT packet", cancellationToken).ConfigureAwait(false);
                break;

            case PingReqPacket:
                // [MQTT-3.12.4-1]
                await _connection.SendAsync(PingRespPacket.Instance, cancellationToken).ConfigureAwait(false);
                break;

            case DisconnectPacket:
                // [MQTT-3.14.4-1]
                Session.MarkCleanDisconnect();
                await _connection.CloseAsync("Client disconnected", cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    public ValueTask OnDisconnectedAsync()
    {
        ClientSession? session = Session;
        if (session is not null)
        {
            Session = null;
            _sessionManager.RemoveSession(session.ClientId, session);
        }

        return ValueTask.CompletedTask;
    }

    private async ValueTask HandleConnectHandshakeAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        if (packet is not ConnectPacket connect)
        {
            // [MQTT-3.1.0-1] First packet MUST be CONNECT; close without CONNACK.
            await _connection.CloseAsync("First packet must be CONNECT", cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!string.Equals(connect.ProtocolName, SupportedProtocolName, StringComparison.Ordinal) ||
            connect.ProtocolLevel != SupportedProtocolLevel)
        {
            // [MQTT-3.1.2-2]
            await _connection.SendAsync(
                new ConnAckPacket
                {
                    SessionPresent = false,
                    ReturnCode = ConnectReturnCode.UnacceptableProtocolVersion
                },
                cancellationToken).ConfigureAwait(false);

            await _connection.CloseAsync("Unacceptable protocol version", cancellationToken).ConfigureAwait(false);
            return;
        }

        SessionRegistrationResult registration = await _sessionManager
            .TryRegisterSessionAsync(connect, _connection, cancellationToken)
            .ConfigureAwait(false);

        if (!registration.Succeeded || registration.Session is null)
        {
            await _connection.SendAsync(
                new ConnAckPacket
                {
                    SessionPresent = false,
                    ReturnCode = registration.ReturnCode
                },
                cancellationToken).ConfigureAwait(false);

            await _connection.CloseAsync($"Connection rejected: {registration.ReturnCode}", cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        Session = registration.Session;

        await _connection.SendAsync(
            new ConnAckPacket
            {
                SessionPresent = false,
                ReturnCode = ConnectReturnCode.Accepted
            },
            cancellationToken).ConfigureAwait(false);
    }
}
