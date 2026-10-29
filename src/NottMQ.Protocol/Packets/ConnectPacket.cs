using NottMQ.Protocol.Primitives;

namespace NottMQ.Protocol.Packets;

/// <summary>
/// MQTT CONNECT packet (OASIS MQTT v3.1.1, section 3.1).
/// </summary>
public record ConnectPacket : MqttPacket
{
    public override PacketType PacketType => PacketType.Connect;

    public string ProtocolName { get; init; } = "MQTT";
    public byte ProtocolLevel { get; init; } = 4; // MQTT 3.1.1 is version 4
    public bool CleanSession { get; init; } = true;
    public ushort KeepAliveSeconds { get; init; } = 60;
    public string ClientId { get; init; } = string.Empty;

    public bool HasWill { get; init; }
    public MqttQoS WillQoS { get; init; } = MqttQoS.AtMostOnce;
    public bool WillRetain { get; init; }
    public string? WillTopic { get; init; }
    public ReadOnlyMemory<byte>? WillMessage { get; init; }

    public string? Username { get; init; }
    public string? Password { get; init; }

    public virtual bool Equals(ConnectPacket? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;

        bool willMsgEqual = (WillMessage == null && other.WillMessage == null) ||
                            (WillMessage != null && other.WillMessage != null &&
                             WillMessage.Value.Span.SequenceEqual(other.WillMessage.Value.Span));

        return ProtocolName == other.ProtocolName &&
               ProtocolLevel == other.ProtocolLevel &&
               CleanSession == other.CleanSession &&
               KeepAliveSeconds == other.KeepAliveSeconds &&
               ClientId == other.ClientId &&
               HasWill == other.HasWill &&
               WillQoS == other.WillQoS &&
               WillRetain == other.WillRetain &&
               WillTopic == other.WillTopic &&
               willMsgEqual &&
               Username == other.Username &&
               Password == other.Password;
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ProtocolName);
        hash.Add(ProtocolLevel);
        hash.Add(CleanSession);
        hash.Add(KeepAliveSeconds);
        hash.Add(ClientId);
        hash.Add(HasWill);
        hash.Add((int)WillQoS);
        hash.Add(WillRetain);
        hash.Add(WillTopic);
        hash.Add(Username);
        hash.Add(Password);
        return hash.ToHashCode();
    }
}
