using NottMQ.Core.Abstractions;
using NottMQ.Protocol.Packets;

namespace NottMQ.Tests.Unit.Core;

internal sealed class FakeClientConnection : IClientConnection
{
    public FakeClientConnection(string? connectionId = null)
    {
        ConnectionId = connectionId ?? Guid.NewGuid().ToString("N");
    }

    public string ConnectionId { get; }
    public List<MqttPacket> SentPackets { get; } = [];
    public bool IsClosed { get; private set; }
    public string? CloseReason { get; private set; }
    public int CloseCount { get; private set; }

    public ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken = default)
    {
        SentPackets.Add(packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default)
    {
        IsClosed = true;
        CloseReason = reason;
        CloseCount++;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        IsClosed = true;
        return ValueTask.CompletedTask;
    }
}
