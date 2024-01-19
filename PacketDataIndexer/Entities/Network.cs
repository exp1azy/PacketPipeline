using PacketDotNet.Utils;
using System.Net;

namespace PacketDataIndexer.Entities
{
    internal class Network
    {
        public byte[] Bytes { get; set; }

        public ByteArraySegment BytesSegment { get; set; }

        public string Checksum { get; set; }

        public string Color { get; set; }

        public IPAddress DestinationAddress { get; set; }

        public bool HasPayloadPacket { get; set; }

        public bool HasPayloadData { get; set; } 

        public byte[] HeaderData { get; set; }

        public ByteArraySegment HeaderDataSegment { get; set; }

        public int HeaderLength { get; set; }

        public ByteArraySegment HeaderSegment { get; set; }
    }
}
