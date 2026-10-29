namespace NottMQ.Protocol.Primitives;

/// <summary>
/// Connect return codes for CONNACK packet (OASIS MQTT v3.1.1, section 3.2.2.3).
/// </summary>
public enum ConnectReturnCode : byte
{
    Accepted = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUsernameOrPassword = 4,
    NotAuthorized = 5
}
