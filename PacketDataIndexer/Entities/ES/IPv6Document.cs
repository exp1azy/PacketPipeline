using PacketDotNet;
using System.Net;

namespace PacketDataIndexer.Entities.ES
{
    internal class IPv6Document : BasePacketDocument
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

        public List<IPv6ExtensionHeader> ExtensionHeaders { get; set; }

        public int ExtensionHeadersLength { get; set; }

        public int FlowLabel { get; set; }

        public ProtocolType NextHeader { get; set; }

        public int TrafficClass { get; set; }
    }

    internal class IPv6ExtensionHeader
    {
        public ProtocolType Header { get; set; }

        public int HeaderExtensionLength { get; set; }

        public ushort Length { get; set; }

        public ProtocolType NextHeader { get; set; }

        public byte[] Payload { get; set; }

        public static explicit operator IPv6ExtensionHeader(PacketDotNet.IPv6ExtensionHeader h) => new IPv6ExtensionHeader
        {
            Header = h.Header,
            Length = h.Length,
            HeaderExtensionLength = h.HeaderExtensionLength,
            NextHeader = h.NextHeader,
            Payload = h.Payload.ActualBytes()
        };
    }
}
