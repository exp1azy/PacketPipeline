using WebSpectre.Shared.ES;

namespace PacketDataIndexer.Models
{
    internal class AgentPackets
    {
        public string Agent { get; set; }

        public List<BasePacketDocument> Packets { get; set; }
    }
}
