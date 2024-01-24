namespace PacketDataIndexer.Entities
{
    internal class Timeval
    {
        public DateTime Date { get; set; }

        public decimal Value { get; set; }

        public ulong Seconds { get; set; }

        public ulong MicroSeconds { get; set; }
    }
}
