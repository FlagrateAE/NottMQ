using System.Buffers;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using MQTTnet;
using MQTTnet.Formatter;
using NottMQ.Core.Configuration;
using NottMQ.Protocol.Codecs;
using NottMQ.Protocol.Packets;
using NottMQ.Server;

namespace NottMQ.Tests.Integration;

public class ConnectLifecycleTests
{
    [Fact]
    public async Task Connect_ValidClient_ReturnsConnAckAccepted()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0
        });
        await broker.StartAsync();

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", broker.Port)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithClientId("it-connect-1")
            .Build();

        MqttClientConnectResult result = await client.ConnectAsync(options);

        result.ResultCode.Should().Be(MqttClientConnectResultCode.Success);
        result.IsSessionPresent.Should().BeFalse();
        client.IsConnected.Should().BeTrue();

        await client.DisconnectAsync();
    }

    [Fact]
    public async Task KeepAlive_ActiveSession_PingReqKeepsClientConnected()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0,
            KeepAliveCheckInterval = TimeSpan.FromMilliseconds(50)
        });
        await broker.StartAsync();

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", broker.Port)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithClientId("it-keepalive-1")
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(1))
            .Build();

        await client.ConnectAsync(options);
        await Task.Delay(TimeSpan.FromSeconds(2.2));

        client.IsConnected.Should().BeTrue();
        await client.DisconnectAsync();
    }

    [Fact]
    public async Task Disconnect_CleanDisconnect_RemovesSession()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0
        });
        await broker.StartAsync();

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", broker.Port)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithClientId("it-disconnect-1")
            .Build();

        await client.ConnectAsync(options);
        broker.SessionManager.ActiveSessionCount.Should().Be(1);

        await client.DisconnectAsync();

        await WaitUntilAsync(() => broker.SessionManager.ActiveSessionCount == 0, TimeSpan.FromSeconds(2));
        broker.SessionManager.ActiveSessionCount.Should().Be(0);
    }

    [Fact]
    public async Task SessionTakeover_SameClientId_DisconnectsPreviousClient()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0
        });
        await broker.StartAsync();

        var factory = new MqttClientFactory();
        using var client1 = factory.CreateMqttClient();
        using var client2 = factory.CreateMqttClient();

        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", broker.Port)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithClientId("shared-client-id")
            .Build();

        await client1.ConnectAsync(options);
        await client2.ConnectAsync(options);

        await WaitUntilAsync(() => !client1.IsConnected, TimeSpan.FromSeconds(2));

        client1.IsConnected.Should().BeFalse();
        client2.IsConnected.Should().BeTrue();
        broker.SessionManager.ActiveSessionCount.Should().Be(1);

        await client2.DisconnectAsync();
    }

    [Fact]
    public async Task KeepAliveTimeout_SilentRawClient_ClosesConnection()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0,
            KeepAliveCheckInterval = TimeSpan.FromMilliseconds(50)
        });
        await broker.StartAsync();

        using var rawClient = new TcpClient();
        await rawClient.ConnectAsync(IPAddress.Loopback, broker.Port);
        await using NetworkStream stream = rawClient.GetStream();

        var buffer = new ArrayBufferWriter<byte>();
        MqttEncoder.Encode(new ConnectPacket
        {
            ClientId = "silent-raw-client",
            CleanSession = true,
            KeepAliveSeconds = 1
        }, buffer);

        await stream.WriteAsync(buffer.WrittenMemory);
        await stream.FlushAsync();

        byte[] connAck = new byte[4];
        using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
        int connAckBytes = await stream.ReadAsync(connAck, readCts.Token);
        connAckBytes.Should().Be(4);

        // Stay silent past 1.5 * 1s; broker should close the connection.
        byte[] probe = new byte[1];
        int bytesRead = await stream.ReadAsync(probe, readCts.Token);
        bytesRead.Should().Be(0);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
        }
    }
}
