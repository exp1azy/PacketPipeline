using PacketDotNet;
using System.Net;

namespace PacketDataIndexer.Entities.ES
{
    internal class IgmpV2Document : BasePacketDocument
    {
        public virtual IgmpMessageType Type { get; set; }

        public short Checksum { get; set; }

        public IPAddress GroupAddress { get; set; }

        public byte MaxResponseTime { get; set; }
    }
}
