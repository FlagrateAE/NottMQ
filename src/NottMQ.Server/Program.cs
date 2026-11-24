using NottMQ.Core.Configuration;
using NottMQ.Server;

int port = 1883;
if (args.Length > 0 && int.TryParse(args[0], out int argPort))
{
    port = argPort;
}
else if (int.TryParse(Environment.GetEnvironmentVariable("NOTTMQ_PORT"), out int envPort))
{
    port = envPort;
}

var options = new BrokerOptions { Port = port };
await using var broker = new NottMQBroker(options);
using var shutdownCts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    shutdownCts.Cancel();
};

await broker.StartAsync(shutdownCts.Token);
Console.WriteLine($"NottMQ MQTT Broker 🐈‍⬛\nListening on {options.Host}:{broker.Port}");

try
{
    await Task.Delay(Timeout.Infinite, shutdownCts.Token);
}
catch (OperationCanceledException)
{
}

await broker.StopAsync();