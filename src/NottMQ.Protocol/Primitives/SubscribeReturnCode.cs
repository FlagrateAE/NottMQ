namespace NottMQ.Protocol.Primitives;

/// <summary>
/// Return codes in SUBACK packet payload (OASIS MQTT v3.1.1, section 3.9.3).
/// </summary>
public enum SubscribeReturnCode : byte
{
    SuccessQoS0 = 0x00,
    SuccessQoS1 = 0x01,
    SuccessQoS2 = 0x02,
    Failure = 0x80
}
