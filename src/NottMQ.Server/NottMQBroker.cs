using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using NottMQ.Core.Configuration;
using NottMQ.Core.Sessions;
using NottMQ.Server.Network;

namespace NottMQ.Server;

/// <summary>
/// Asynchronous TCP MQTT broker hosting client connections and session lifecycle.
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed class NottMQBroker(BrokerOptions? options = null) : IAsyncDisposable
{
    private readonly BrokerOptions _options = options ?? new BrokerOptions();
    private readonly ConcurrentDictionary<string, Task> _activeConnections = new(StringComparer.Ordinal);

    private TcpListener? _listener;
    private CancellationTokenSource? _shutdownCts;
    private Task? _acceptLoopTask;
    private int _started;
    private int _stopped;

    public int Port { get; private set; }
    public SessionManager SessionManager { get; } = new();

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            throw new InvalidOperationException("Broker has already been started.");
        }

        _shutdownCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener = new TcpListener(_options.Host, _options.Port);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

        _acceptLoopTask = AcceptLoopAsync(_shutdownCts.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) == 1)
        {
            return;
        }

        if (_shutdownCts is not null)
        {
            await _shutdownCts.CancelAsync().ConfigureAwait(false);
        }

        _listener?.Stop();

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        var remaining = _activeConnections.Values.ToArray();
        if (remaining.Length > 0)
        {
            await Task.WhenAll(remaining).ConfigureAwait(false);
        }

        _shutdownCts?.Dispose();
        _shutdownCts = null;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient tcpClient;
            try
            {
                tcpClient = await _listener!.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            tcpClient.NoDelay = true;
            var connection = new TcpClientConnection(tcpClient, SessionManager, _options);
            var connectionTask = connection.RunAsync(cancellationToken);
            _activeConnections[connection.ConnectionId] = connectionTask;

            _ = connectionTask.ContinueWith(
                static (_, state) =>
                {
                    var entry = (ConnectionEntry)state!;
                    entry.Connections.TryRemove(entry.ConnectionId, out var _);
                },
                new ConnectionEntry(_activeConnections, connection.ConnectionId),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private sealed record ConnectionEntry(
        ConcurrentDictionary<string, Task> Connections,
        string ConnectionId);
}
