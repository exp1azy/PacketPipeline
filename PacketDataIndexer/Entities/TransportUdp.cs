using PacketDotNet.Utils;
using PacketDotNet;

namespace PacketDataIndexer.Entities
{
    internal class TransportUdp
    {
        public byte[] Bytes { get; set; }

        public ByteArraySegment BytesSegment { get; set; }

        public ushort Checksum { get; set; }

        public string Color { get; set; }

        public ushort DestinationPort { get; set; }

        public bool HasPayloadPacket { get; set; }

        public bool HasPayloadData { get; set; }

        public byte[] HeaderData { get; set; }

        public ByteArraySegment HeaderDataSegment { get; set; }

        public ByteArraySegment HeaderSegment { get; set; }

        public bool IsPayloadInitialized { get; set; }

        public int Length { get; set; }

        public Packet ParentPacket { get; set; }

        public byte[] PayloadData { get; set; }

        public ByteArraySegment PayloadDataSegment { get; set; }

        public Packet PayloadPacket { get; set; }

        public ushort SourcePort { get; set; }

        public int TotalPacketLength { get; set; }

        public bool ValidChecksum { get; set; }
    }
}
