using System.Net;
using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using MQTTnet;
using MQTTnet.Formatter;
using NottMQ.Core.Configuration;
using NottMQ.Server;

namespace NottMQ.Tests.Integration;

public class ConnectionFuzzTests
{
    [Fact]
    public async Task HttpGetPayload_ClosesClientSocket()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0,
            ConnectTimeout = TimeSpan.FromMilliseconds(500)
        });
        await broker.StartAsync();

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, broker.Port);
        await using NetworkStream stream = client.GetStream();

        byte[] httpPayload = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"u8.ToArray();
        await stream.WriteAsync(httpPayload);
        await stream.FlushAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        byte[] buffer = new byte[64];
        int read = await stream.ReadAsync(buffer, cts.Token);

        read.Should().Be(0);
    }

    [Fact]
    public async Task RandomBytesPayload_ClosesClientSocket()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0,
            ConnectTimeout = TimeSpan.FromMilliseconds(300)
        });
        await broker.StartAsync();

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, broker.Port);
        await using NetworkStream stream = client.GetStream();

        byte[] randomBytes = new byte[256];
        new Random(42).NextBytes(randomBytes);
        await stream.WriteAsync(randomBytes);
        await stream.FlushAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        byte[] buffer = new byte[64];
        int read = await stream.ReadAsync(buffer, cts.Token);

        read.Should().Be(0);
    }

    [Fact]
    public async Task TruncatedLargeRemainingLength_DoesNotCrashAndAllowsSubsequentClient()
    {
        await using var broker = new NottMQBroker(new BrokerOptions
        {
            Host = IPAddress.Loopback,
            Port = 0
        });
        await broker.StartAsync();

        using (var malformedClient = new TcpClient())
        {
            await malformedClient.ConnectAsync(IPAddress.Loopback, broker.Port);
            await using NetworkStream stream = malformedClient.GetStream();
            // CONNECT fixed header + 2 bytes of multi-byte RemainingLength, then abrupt close.
            await stream.WriteAsync(new byte[] { 0x10, 0xFF });
            await stream.FlushAsync();
        }

        var factory = new MqttClientFactory();
        using var validClient = factory.CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", broker.Port)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithClientId("valid-after-fuzz")
            .Build();

        var result = await validClient.ConnectAsync(options);

        result.ResultCode.Should().Be(MqttClientConnectResultCode.Success);
        await validClient.DisconnectAsync();
    }
}
