namespace NottMQ.Protocol.Exceptions;

public class MqttProtocolException : Exception
{
    public MqttProtocolException(string message) : base(message)
    {
    }

    public MqttProtocolException(string message, Exception innerException) : base(message, innerException)
    {
    }
}