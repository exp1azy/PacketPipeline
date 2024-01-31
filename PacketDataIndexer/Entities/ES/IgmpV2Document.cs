namespace PacketDataIndexer.Entities.ES
{
    internal class IgmpV2Document : BasePacketDocument
    {
        public virtual string Type { get; set; }

        public short Checksum { get; set; }

        public string GroupAddress { get; set; }

        public byte MaxResponseTime { get; set; }
    }
}
