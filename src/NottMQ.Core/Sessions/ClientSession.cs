using NottMQ.Core.Abstractions;

namespace NottMQ.Core.Sessions;

/// <summary>
/// Represents an active MQTT client session and its connection lifecycle state.
/// </summary>
public sealed class ClientSession
{
    private long _lastActivityTicks;
    private int _isCleanDisconnect;

    public ClientSession(
        string clientId,
        bool cleanSession,
        ushort keepAliveSeconds,
        IClientConnection connection,
        DateTime? connectedAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentNullException.ThrowIfNull(connection);

        ClientId = clientId;
        CleanSession = cleanSession;
        KeepAliveSeconds = keepAliveSeconds;
        Connection = connection;

        DateTime now = connectedAtUtc ?? DateTime.UtcNow;
        ConnectedAtUtc = now;
        _lastActivityTicks = now.Ticks;
    }

    public string ClientId { get; }
    public bool CleanSession { get; }
    public ushort KeepAliveSeconds { get; }
    public DateTime ConnectedAtUtc { get; }
    public DateTime LastActivityUtc => new(Volatile.Read(ref _lastActivityTicks), DateTimeKind.Utc);
    public bool IsCleanDisconnect => Volatile.Read(ref _isCleanDisconnect) == 1;
    public IClientConnection Connection { get; }

    public void Touch()
    {
        Volatile.Write(ref _lastActivityTicks, DateTime.UtcNow.Ticks);
    }

    public void MarkCleanDisconnect()
    {
        Volatile.Write(ref _isCleanDisconnect, 1);
    }

    public bool HasKeepAliveExpired(DateTime utcNow, double multiplier)
    {
        if (KeepAliveSeconds == 0)
        {
            return false;
        }

        double timeoutSeconds = KeepAliveSeconds * multiplier;
        return (utcNow - LastActivityUtc).TotalSeconds > timeoutSeconds;
    }
}
