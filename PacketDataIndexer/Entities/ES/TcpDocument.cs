using PacketDotNet.Tcp;

namespace PacketDataIndexer.Entities.ES
{
    internal class TcpDocument : BasePacketDocument
    {
        public bool Acknowledgment { get; set; }

        public uint AcknowledgmentNumber { get; set; }

        public ushort Checksum { get; set; }

        public bool CongestionWindowReduced { get; set; }

        public int DataOffset { get; set; }

        public ushort DestinationPort { get; set; }

        public bool ExplicitCongestionNotificationEcho { get; set; }

        public bool Finished { get; set; }

        public ushort Flags { get; set; }

        public bool NonceSum { get; set; }

        public byte[] Options { get; set; }

        public List<TcpOption> OptionsCollection { get; set; }

        public byte[] OptionsSegment { get; set; }

        public bool Push { get; set; }

        public bool Reset { get; set; }

        public uint SequenceNumber { get; set; }

        public ushort SourcePort { get; set; }

        public bool Synchronize { get; set; }

        public bool Urgent { get; set; }

        public int UrgentPointer { get; set; }

        public bool ValidChecksum { get; set; }

        public bool ValidTcpChecksum { get; set; }

        public ushort WindowSize { get; set; }
    }

    internal class TcpOption
    {
        public byte[] Bytes { get; set; }

        public string Kind { get; set; }

        public byte Length { get; set; }

        public static explicit operator TcpOption(PacketDotNet.Tcp.TcpOption option) => new TcpOption
        {
            Bytes = option.Bytes,
            Kind = option.Kind.ToString(),
            Length = option.Length,
        };     
    } 
}
