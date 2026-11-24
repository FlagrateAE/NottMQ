using NottMQ.Protocol.Packets;

namespace NottMQ.Core.Abstractions;

/// <summary>
/// Transport-agnostic abstraction over a connected MQTT client.
/// </summary>
public interface IClientConnection : IAsyncDisposable
{
    string ConnectionId { get; }
    ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken = default);
    ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default);
}
