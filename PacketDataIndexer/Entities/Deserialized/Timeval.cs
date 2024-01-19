using Newtonsoft.Json;

namespace PacketDataIndexer.Entities.Deserialized
{
    internal class Timeval
    {
        [JsonProperty("Seconds")]
        public long Seconds { get; set; }

        [JsonProperty("MicroSeconds")]
        public long MicroSeconds { get; set; }

        [JsonProperty("Value")]
        public double Value { get; set; }

        [JsonProperty("Date")]
        public DateTime Date { get; set; }
    }
}