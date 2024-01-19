using Newtonsoft.Json;

namespace PacketDataIndexer.Entities.Deserialized.RawPacket
{
    internal class RawPacket
    {
        [JsonProperty("Data")]
        public byte[] Data { get; set; }

        [JsonProperty("LinkLayerType")]
        public ushort LinkLayerType { get; set; }

        [JsonProperty("Timeval")]
        public Timeval Timeval { get; set; }

        [JsonProperty("PacketLength")]
        public int PacketLength { get; set; }
    }
}
