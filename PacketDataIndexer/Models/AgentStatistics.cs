using WebSpectre.Shared.ES;

namespace PacketDataIndexer.Models
{
    internal class AgentStatistics
    {
        public string Agent { get; set; }

        public List<StatisticsDocument> Statistics { get; set; }
    }
}
