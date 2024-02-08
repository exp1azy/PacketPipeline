using PacketDotNet;
using PacketDotNet.Utils;
using System.Net;
using System.Net.NetworkInformation;

namespace PacketTests
{
    public class PacketGenerationTests
    {
        [Fact]
        public void GenerateAndExtractTcpTest()
        {
            var bytes = GenerateTCP();
            var packet = Packet.ParsePacket(LinkLayers.Ethernet, bytes);
            var extractedPacket = GetTransport(packet);

            Assert.NotNull(extractedPacket);
            Assert.True(extractedPacket is TcpPacket);
        }

        [Fact]
        public void GenerateAndExtractUdpTest()
        {
            var bytes = GenerateUDP();
            var packet = Packet.ParsePacket(LinkLayers.Ethernet, bytes);
            var extractedPacket = GetTransport(packet);

            Assert.NotNull(extractedPacket);
            Assert.True(extractedPacket is UdpPacket);
        }

        [Fact]
        public void GenerateAndExtractIcmpTest()
        {
            var bytes = GenerateICMPv4();
            var packet = Packet.ParsePacket(LinkLayers.Ethernet, bytes);
            var extractedPacket = GetInternet(packet);

            Assert.NotNull(extractedPacket);
            Assert.True(extractedPacket is IcmpV4Packet);
        }

        [Fact]
        public void GenerateAndExtractIgmpTest()
        {
            var bytes = GenerateIGMPv2();
            var packet = Packet.ParsePacket(LinkLayers.Ethernet, bytes);
            var extractedPacket = GetInternet(packet);

            Assert.NotNull(extractedPacket);
            Assert.True(extractedPacket is IgmpV2Packet);
        }

        private dynamic GetTransport(Packet packet) => 
            packet.Extract<TcpPacket>() ?? (dynamic)packet.Extract<UdpPacket>();

        private dynamic GetInternet(Packet packet) =>
            packet.Extract<IcmpV4Packet>() ?? packet.Extract<IcmpV6Packet>() ??
            packet.Extract<IgmpV2Packet>() ?? packet.Extract<IPv4Packet>() ?? (dynamic)packet.Extract<IPv6Packet>();

        private EthernetPacket GetEthernet() => new EthernetPacket (
            new PhysicalAddress([00, 11, 22, 33, 44, 55]),
            new PhysicalAddress([55, 44, 33, 22, 11, 00]),
            EthernetType.IPv4
        );

        private IPv4Packet GetIP() => new IPv4Packet (
            new IPAddress(0x0A010101),
            new IPAddress(0xC0A80101)
        );

        private byte[] GenerateTCP()
        {
            var rand = new Random();
       
            var tcpPacket = new TcpPacket((ushort)rand.Next(ushort.MinValue, ushort.MaxValue), (ushort)rand.Next(ushort.MinValue, ushort.MaxValue));
            byte[] data = new byte[10];
            rand.NextBytes(data);
            tcpPacket.PayloadData = data;

            var ethernetPacket = GetEthernet();
            var ipPacket = GetIP();

            ipPacket.PayloadPacket = tcpPacket;
            ethernetPacket.PayloadPacket = ipPacket;

            return ethernetPacket.Bytes;
        }

        private byte[] GenerateUDP()
        {
            var rand = new Random();

            var udpPacket = new UdpPacket((ushort)rand.Next(ushort.MinValue, ushort.MaxValue), (ushort)rand.Next(ushort.MinValue, ushort.MaxValue));
            byte[] data = new byte[10];
            rand.NextBytes(data);
            udpPacket.PayloadData = data;

            var ethernetPacket = GetEthernet();
            var ipPacket = GetIP();

            ipPacket.PayloadPacket = udpPacket;
            ethernetPacket.PayloadPacket = ipPacket;

            return ethernetPacket.Bytes;
        }

        private byte[] GenerateICMPv4()
        {
            var rand = new Random();

            byte[] byteArraySegment = new byte[10];
            rand.NextBytes(byteArraySegment);

            var icmpPacket = new IcmpV4Packet(new ByteArraySegment(byteArraySegment));
            icmpPacket.TypeCode = IcmpV4TypeCode.EchoRequest;

            var ethernetPacket = GetEthernet();
            var ipPacket = GetIP();

            ipPacket.PayloadPacket = icmpPacket;
            ethernetPacket.PayloadPacket = ipPacket;

            return ethernetPacket.Bytes;
        }

        private byte[] GenerateIGMPv2()
        {
            var rand = new Random();

            byte[] byteArraySegment = new byte[10];
            rand.NextBytes(byteArraySegment);

            var igmpPacket = new IgmpV2Packet(new ByteArraySegment(byteArraySegment));
            igmpPacket.Type = IgmpMessageType.MembershipReportIGMPv2;

            var ethernetPacket = GetEthernet();
            var ipPacket = GetIP();

            ipPacket.PayloadPacket = igmpPacket;
            ethernetPacket.PayloadPacket = ipPacket;

            return ethernetPacket.Bytes;
        }
    }
}