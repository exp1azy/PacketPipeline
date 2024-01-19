using Newtonsoft.Json;
using SharpPcap;

namespace PacketDataIndexer.Entities.Deserialized.Statistics
{
    internal class Statistics
    {
        [JsonProperty("Device")]
        public Device Device { get; set; }

        [JsonProperty("Timeval")]
        public PosixTimeval Timeval { get; set; }

        [JsonProperty("ReceivedPackets")]
        public long ReceivedPackets { get; set; }

        [JsonProperty("ReceivedBytes")]
        public long ReceivedBytes { get; set; }
    }
}
