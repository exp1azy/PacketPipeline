using PacketDotNet;
using System.Net;

namespace PacketDataIndexer.Entities.ES
{
    internal class IPv4Document : BasePacketDocument
    {
        public IPAddress DestinationAddress { get; set; }

        public int HeaderLength { get; set; }

        public int HopLimit { get; set; }

        public ushort PayloadLength { get; set; }

        public ProtocolType Protocol { get; set; }

        public IPAddress SourceAddress { get; set; }

        public int TimeToLive { get; set; }

        public int TotalLength { get; set; }

        public IPVersion Version { get; set; }

        public ushort Checksum { get; set; }

        public int DifferentiatedServices { get; set; }

        public int FragmentFlags { get; set; }

        public int FragmentOffset { get; set; }

        public ushort IPv4Id { get; set; }

        public int TypeOfService { get; set; }

        public bool ValidChecksum { get; set; }

        public bool ValidIPChecksum { get; set; }
    }
}
