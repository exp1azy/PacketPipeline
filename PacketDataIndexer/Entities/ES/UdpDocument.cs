namespace PacketDataIndexer.Entities.ES
{
    internal class UdpDocument : BasePacketDocument
    {
        public ushort Checksum { get; set; }

        public ushort DestinationPort { get; set; }

        public int Length { get; set; }

        public ushort SourcePort { get; set; }

        public bool ValidChecksum { get; set; }

        public bool ValidUdpChecksum { get; set; }
    }
}
