using PacketDotNet;

namespace PacketDataIndexer.Entities
{
    internal class Document
    {
        public Guid Id { get; set; }

        public string Agent { get; set; }

        public TcpPacket? TcpPacket { get; set; }

        public UdpPacket? UdpPacket { get; set; }

        public IPv4Packet? IPv4Packet { get; set; }

        public IPv6Packet? IPv6Packet { get; set; }

        public IgmpV2Packet? IgmpV2Packet { get; set; }

        public IcmpV4Packet? IcmpV4Packet { get; set; }

        public IcmpV6Packet? IcmpV6Packet { get; set; }
    }
}
