using Newtonsoft.Json;
using SharpPcap;

namespace PacketDataIndexer.Entities.Deserialized.RawPacket
{
    internal class RawPacket
    {
        [JsonProperty("Data")]
        public byte[] Data { get; set; }

        [JsonProperty("LinkLayerType")]
        public ushort LinkLayerType { get; set; }

        [JsonProperty("Timeval")]
        public PosixTimeval Timeval { get; set; }

        [JsonProperty("PacketLength")]
        public int PacketLength { get; set; }
    }
}
