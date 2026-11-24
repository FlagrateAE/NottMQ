using System.Collections.Concurrent;
using NottMQ.Core.Abstractions;
using NottMQ.Protocol.Packets;
using NottMQ.Protocol.Primitives;

namespace NottMQ.Core.Sessions;

public readonly record struct SessionRegistrationResult(
    bool Succeeded,
    ConnectReturnCode ReturnCode,
    ClientSession? Session)
{
    public static SessionRegistrationResult Accepted(ClientSession session) =>
        new(true, ConnectReturnCode.Accepted, session);

    public static SessionRegistrationResult Rejected(ConnectReturnCode returnCode) =>
        new(false, returnCode, null);
}

/// <summary>
/// Thread-safe registry of active MQTT client sessions, handling ClientId validation and session takeover.
/// </summary>
public sealed class SessionManager
{
    private readonly ConcurrentDictionary<string, ClientSession> _sessions = new(StringComparer.Ordinal);

    public int ActiveSessionCount => _sessions.Count;

    public bool TryGetSession(string clientId, out ClientSession? session) =>
        _sessions.TryGetValue(clientId, out session);

    public async ValueTask<SessionRegistrationResult> TryRegisterSessionAsync(
        ConnectPacket connect,
        IClientConnection connection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connect);
        ArgumentNullException.ThrowIfNull(connection);

        string clientId = connect.ClientId;

        if (string.IsNullOrEmpty(clientId))
        {
            if (!connect.CleanSession)
            {
                // [MQTT-3.1.3-8]
                return SessionRegistrationResult.Rejected(ConnectReturnCode.IdentifierRejected);
            }

            // [MQTT-3.1.3-6], [MQTT-3.1.3-7]
            clientId = $"nottmq-{Guid.NewGuid():N}";
        }

        var newSession = new ClientSession(clientId, connect.CleanSession, connect.KeepAliveSeconds, connection);

        while (true)
        {
            if (_sessions.TryGetValue(clientId, out ClientSession? existingSession))
            {
                // [MQTT-3.1.4-2] Disconnect existing client before registering the new session.
                if (_sessions.TryRemove(new KeyValuePair<string, ClientSession>(clientId, existingSession)))
                {
                    await existingSession.Connection.CloseAsync("Session takeover", cancellationToken)
                        .ConfigureAwait(false);
                }

                continue;
            }

            if (_sessions.TryAdd(clientId, newSession))
            {
                return SessionRegistrationResult.Accepted(newSession);
            }
        }
    }

    public bool RemoveSession(string clientId, ClientSession expectedSession)
    {
        ArgumentException.ThrowIfNullOrEmpty(clientId);
        ArgumentNullException.ThrowIfNull(expectedSession);

        return _sessions.TryRemove(new KeyValuePair<string, ClientSession>(clientId, expectedSession));
    }
}
