using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.ES;
using PacketDataIndexer.Interfaces;
using PacketDotNet;

namespace PacketDataIndexer.Services
{
    internal class TransportLayerPacketHandler : INetwork
    {
        public object? Extract(Packet packet) =>
            packet.Extract<TcpPacket>() ?? (object)packet.Extract<UdpPacket>();      

        public BasePacketDocument? GenerateDocument(object packet, Guid? transportId, Guid? networkId, string agent)
        {
            var model = OSIModel.Transport.ToString();

            if (packet is TcpPacket)
            {
                TcpPacket tcp = (TcpPacket)packet;

                return new TcpDocument
                {
                    Id = (Guid)transportId!,
                    Nested = networkId,
                    Agent = agent,
                    Model = model,
                    Acknowledgment = tcp.Acknowledgment,
                    AcknowledgmentNumber = tcp.AcknowledgmentNumber,
                    Bytes = tcp.Bytes,
                    Checksum = tcp.Checksum,
                    Color = tcp.Color,
                    CongestionWindowReduced = tcp.CongestionWindowReduced,
                    DataOffset = tcp.DataOffset,
                    DestinationPort = tcp.DestinationPort,
                    ExplicitCongestionNotificationEcho = tcp.ExplicitCongestionNotificationEcho,
                    Finished = tcp.Finished,
                    Flags = tcp.Flags,
                    HasPayloadData = tcp.HasPayloadData,
                    HasPayloadPacket = tcp.HasPayloadPacket,
                    HeaderData = tcp.HeaderData,
                    IsPayloadInitialized = tcp.IsPayloadInitialized,
                    NonceSum = tcp.NonceSum,
                    Options = tcp.Options,
                    OptionsCollection = tcp.OptionsCollection == null ? null : tcp.OptionsCollection.Select(o => (TcpOption)o).ToList(),
                    OptionsSegment = tcp.OptionsSegment.ActualBytes(),
                    PayloadData = tcp.PayloadData,
                    Push = tcp.Push,
                    Reset = tcp.Reset,
                    SequenceNumber = tcp.SequenceNumber,
                    SourcePort = tcp.SourcePort,
                    Synchronize = tcp.Synchronize,
                    TotalPacketLength = tcp.TotalPacketLength,
                    Urgent = tcp.Urgent,
                    UrgentPointer = tcp.UrgentPointer,
                    ValidChecksum = tcp.ValidChecksum,
                    ValidTcpChecksum = tcp.ValidTcpChecksum,
                    WindowSize = tcp.WindowSize,
                };
            }
            else if (packet is UdpPacket)
            {
                UdpPacket udp = (UdpPacket)packet;

                return new UdpDocument
                {
                    Id = (Guid)transportId!,
                    Nested = networkId,
                    Agent = agent,
                    Model = model,
                    Bytes = udp.Bytes,
                    Checksum = udp.Checksum,
                    Color = udp.Color,
                    DestinationPort = udp.DestinationPort,
                    HasPayloadData = udp.HasPayloadData,
                    HasPayloadPacket = udp.HasPayloadPacket,
                    HeaderData = udp.HeaderData,
                    IsPayloadInitialized = udp.IsPayloadInitialized,
                    Length = udp.Length,
                    PayloadData = udp.PayloadData,
                    SourcePort = udp.SourcePort,
                    TotalPacketLength = udp.TotalPacketLength,
                    ValidChecksum = udp.ValidChecksum,
                    ValidUdpChecksum = udp.ValidUdpChecksum,
                };
            }
            else return null;
        }
    }
}
