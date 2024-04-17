using Nest;
using WebSpectre.Shared.ES;

namespace PacketDataIndexer.Services
{
    public static class Indexator
    {
        /// <summary>
        /// Индексация метрик сети.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="pcapStat"></param>
        public static void IndexPcapMetrics(BulkDescriptor bulkDescriptor, PcapMetricsDocument pcapStat)
        {
            bulkDescriptor.Index<PcapMetricsDocument>(s => s
                .Document(pcapStat)
                .Id(pcapStat.Id)
                .Index("pcap_metrics")
            );
        }

        /// <summary>
        /// Индексация статистики.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="statistics"></param>
        public static void IndexStatistics(BulkDescriptor bulkDescriptor, StatisticsDocument statistics)
        {
            bulkDescriptor.Index<StatisticsDocument>(s => s
                .Document(statistics)
                .Id(statistics.Id)
                .Index("statistics")
            );
        }

        /// <summary>
        /// Индексация TCP.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="tcp"></param>
        public static void IndexTcp(BulkDescriptor bulkDescriptor, TcpDocument tcp) =>
            bulkDescriptor.Index<TcpDocument>(s => s
                .Document(tcp)
                .Id(tcp.Id)
                .Index("tcp")
            );

        /// <summary>
        /// Индексация UDP.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="udp"></param>
        public static void IndexUdp(BulkDescriptor bulkDescriptor, UdpDocument udp) =>
            bulkDescriptor.Index<UdpDocument>(s => s
                .Document(udp)
                .Id(udp.Id)
                .Index("udp")
            );

        /// <summary>
        /// Индексация IPv4.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="ipv4"></param>
        public static void IndexIPv4(BulkDescriptor bulkDescriptor, IPv4Document ipv4) =>
            bulkDescriptor.Index<IPv4Document>(s => s
                .Document(ipv4)
                .Id(ipv4.Id)
                .Index("ipv4")
            );

        /// <summary>
        /// Индексация IPv6.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="ipv6"></param>
        public static void IndexIPv6(BulkDescriptor bulkDescriptor, IPv6Document ipv6) =>
            bulkDescriptor.Index<IPv6Document>(s => s
                .Document(ipv6)
                .Id(ipv6.Id)
                .Index("ipv6")
            );

        /// <summary>
        /// Индексация IcmpV4.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="icmpv4"></param>
        public static void IndexIcmpV4(BulkDescriptor bulkDescriptor, IcmpV4Document icmpv4) =>
            bulkDescriptor.Index<IcmpV4Document>(s => s
                .Document(icmpv4)
                .Id(icmpv4.Id)
                .Index("icmpv4")
            );

        /// <summary>
        /// Индексация IcmpV6.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="icmpv6"></param>
        public static void IndexIcmpV6(BulkDescriptor bulkDescriptor, IcmpV6Document icmpv6) =>
            bulkDescriptor.Index<IcmpV6Document>(s => s
                .Document(icmpv6)
                .Id(icmpv6.Id)
                .Index("icmpv6")
            );

        /// <summary>
        /// Индексация IgmpV2.
        /// </summary>
        /// <param name="bulkDescriptor"></param>
        /// <param name="igmp"></param>
        public static void IndexIgmpV2(BulkDescriptor bulkDescriptor, IgmpV2Document igmp) =>
            bulkDescriptor.Index<IgmpV2Document>(s => s
                .Document(igmp)
                .Id(igmp.Id)
                .Index("igmp")
            );
    }
}
