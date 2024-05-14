using WebSpectre.Shared.ES;

namespace PacketDataIndexer.Models
{
    internal class AgentMetrics
    {
        public string Agent { get; set; }

        public List<PcapMetricsDocument> Metrics { get; set; }
    }
}
