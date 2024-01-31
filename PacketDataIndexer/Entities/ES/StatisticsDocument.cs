namespace PacketDataIndexer.Entities.ES
{
    internal class StatisticsDocument
    {
        public Guid Id { get; set; }

        public string Agent { get; set; }

        public Statistics Statistics { get; set; }
    }
}
