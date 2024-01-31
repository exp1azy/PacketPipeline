namespace PacketDataIndexer.Entities.ES
{
    internal class IcmpV4Document : BasePacketDocument
    {
        public ushort Checksum { get; set; }

        public byte[] Data { get; set; }

        public ushort IcmpV4Id { get; set; }

        public ushort Sequence { get; set; }

        public string TypeCode { get; set; }

        public bool ValidIcmpChecksum { get; set; }
    }
}
