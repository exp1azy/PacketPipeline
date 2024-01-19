using Newtonsoft.Json;

namespace PacketDataIndexer.Entities.Deserialized.Statistics
{
    internal class Device
    {
        [JsonProperty("Name")]
        public string Name { get; set; }

        [JsonProperty("Description")]
        public string Description { get; set; }

        [JsonProperty("LastError")]
        public string LastError { get; set; }

        [JsonProperty("Filter")]
        public string Filter { get; set; }

        [JsonProperty("Statistics")]
        public DeviceStatistics Statistics { get; set; }

        [JsonProperty("LinkType")]
        public ushort LinkType { get; set; }
    }
}