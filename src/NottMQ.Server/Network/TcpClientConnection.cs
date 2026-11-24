using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using NottMQ.Core.Abstractions;
using NottMQ.Core.Configuration;
using NottMQ.Core.Sessions;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Exceptions;
using NottMQ.Protocol.Packets;

namespace NottMQ.Server.Network;

/// <summary>
/// Manages a single TCP client connection using <see cref="System.IO.Pipelines"/>.
/// </summary>
public sealed class TcpClientConnection : IClientConnection
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _stream;
    private readonly PipeReader _pipeReader;
    private readonly PipeWriter _pipeWriter;
    private readonly BrokerOptions _options;
    private readonly MqttConnectionHandler _handler;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly CancellationTokenSource _connectionCts = new();

    private int _closed;

    public TcpClientConnection(
        TcpClient tcpClient,
        SessionManager sessionManager,
        BrokerOptions options)
    {
        ArgumentNullException.ThrowIfNull(tcpClient);
        ArgumentNullException.ThrowIfNull(sessionManager);
        ArgumentNullException.ThrowIfNull(options);

        _tcpClient = tcpClient;
        _options = options;
        ConnectionId = Guid.NewGuid().ToString("N");

        _stream = tcpClient.GetStream();
        _pipeReader = PipeReader.Create(_stream);
        _pipeWriter = PipeWriter.Create(_stream);
        _handler = new MqttConnectionHandler(this, sessionManager, options);
    }

    public string ConnectionId { get; }

    public async Task RunAsync(CancellationToken brokerShutdownToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            brokerShutdownToken,
            _connectionCts.Token);

        using var connectTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(linkedCts.Token);
        Task? connectTimeoutTask = _options.ConnectTimeout > TimeSpan.Zero
            ? EnforceConnectTimeoutAsync(connectTimeoutCts.Token)
            : null;
        Task? watchdogTask = null;

        try
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                ReadResult result = await _pipeReader.ReadAsync(linkedCts.Token).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                if (buffer.Length > _options.MaxPacketSize)
                {
                    await CloseAsync("Packet exceeds MaxPacketSize", CancellationToken.None).ConfigureAwait(false);
                    break;
                }

                SequencePosition consumed = buffer.Start;
                SequencePosition examined = buffer.End;

                while (MqttDecoder.TryDecode(
                           buffer,
                           out MqttPacket? packet,
                           out SequencePosition packetConsumed,
                           out _))
                {
                    consumed = packetConsumed;
                    buffer = buffer.Slice(packetConsumed);

                    if (packet is not null)
                    {
                        bool hadSession = _handler.Session is not null;
                        await _handler.HandlePacketAsync(packet, linkedCts.Token).ConfigureAwait(false);

                        if (!hadSession && _handler.Session is not null)
                        {
                            await connectTimeoutCts.CancelAsync().ConfigureAwait(false);

                            if (_handler.Session.KeepAliveSeconds > 0)
                            {
                                watchdogTask = RunKeepAliveWatchdogAsync(_handler.Session, linkedCts.Token);
                            }
                        }
                    }

                    if (Volatile.Read(ref _closed) == 1)
                    {
                        examined = consumed;
                        break;
                    }
                }

                _pipeReader.AdvanceTo(consumed, examined);

                if (result.IsCompleted || Volatile.Read(ref _closed) == 1)
                {
                    break;
                }
            }
        }
        catch (MqttProtocolException)
        {
            await CloseAsync("Protocol error", CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Normal connection close or broker shutdown.
        }
        catch (IOException)
        {
            // Underlying network stream closed or reset.
        }
        catch (SocketException)
        {
            // Underlying socket error.
        }
        catch (ObjectDisposedException)
        {
            // Connection disposed during shutdown.
        }
        finally
        {
            await CloseAsync("Connection terminated", CancellationToken.None).ConfigureAwait(false);

            await connectTimeoutCts.CancelAsync().ConfigureAwait(false);
            if (connectTimeoutTask is not null)
            {
                try { await connectTimeoutTask.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
            }

            if (watchdogTask is not null)
            {
                try { await watchdogTask.ConfigureAwait(false); }
                catch (OperationCanceledException) { }
            }

            await _handler.OnDisconnectedAsync().ConfigureAwait(false);
            await _pipeReader.CompleteAsync().ConfigureAwait(false);
            await _pipeWriter.CompleteAsync().ConfigureAwait(false);
            _tcpClient.Dispose();
            _connectionCts.Dispose();
            _writeLock.Dispose();
        }
    }

    public async ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(packet);

        if (Volatile.Read(ref _closed) == 1)
        {
            return;
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _closed) == 1)
            {
                return;
            }

            MqttEncoder.Encode(packet, _pipeWriter);
            await _pipeWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
        {
            await CloseAsync("Write failed", CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public ValueTask CloseAsync(string reason, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _closed, 1) == 1)
        {
            return ValueTask.CompletedTask;
        }

        try
        {
            _connectionCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            _tcpClient.Client?.Shutdown(SocketShutdown.Both);
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
        {
        }

        _tcpClient.Close();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => CloseAsync("Disposed");

    private async Task EnforceConnectTimeoutAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(_options.ConnectTimeout, cancellationToken).ConfigureAwait(false);

            if (_handler.Session is null)
            {
                await CloseAsync("Connect timeout", CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task RunKeepAliveWatchdogAsync(ClientSession session, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.KeepAliveCheckInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (session.HasKeepAliveExpired(DateTime.UtcNow, _options.KeepAliveMultiplier))
                {
                    await CloseAsync("KeepAlive timeout", CancellationToken.None).ConfigureAwait(false);
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
