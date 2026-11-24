using System.Net;

namespace NottMQ.Core.Configuration;

/// <summary>
/// Configuration options for the NottMQ broker.
/// </summary>
public sealed class BrokerOptions
{
    public IPAddress Host { get; set; } = IPAddress.Any;
    public int Port { get; set; } = 1883;
    public int MaxPacketSize { get; set; } = 1_048_576;
    public double KeepAliveMultiplier { get; set; } = 1.5;
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan KeepAliveCheckInterval { get; set; } = TimeSpan.FromMilliseconds(250);
}
