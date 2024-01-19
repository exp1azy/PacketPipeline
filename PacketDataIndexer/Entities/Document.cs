namespace PacketDataIndexer.Entities
{
    internal class Document
    {
        public Guid Id { get; set; }

        public string Agent { get; set; }

        public string TProtocol { get; set; }

        public string NProtocol { get; set; }

        public TransportUdp? TransportUdp { get; set; }

        public Network Network { get; set; }
    }
}
