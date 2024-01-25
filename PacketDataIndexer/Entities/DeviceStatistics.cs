using Newtonsoft.Json;

namespace PacketDataIndexer.Entities
{
    internal class DeviceStatistics
    {
        [JsonProperty("ReceivedPackets")]
        public long ReceivedPackets { get; set; }

        [JsonProperty("DroppedPackets")]
        public long DroppedPackets { get; set; }

        [JsonProperty("InterfaceDroppedPackets")]
        public long InterfaceDroppedPackets { get; set; }
    }
}