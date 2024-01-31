using Nest;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.ES;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;
using System.Collections.Concurrent;
using Error = PacketDataIndexer.Resources.Error;

namespace PacketDataIndexer
{
    /// <summary>
    /// Конвейер пакетов.
    /// </summary>
    internal class PacketPipeline : BackgroundService
    {
        private readonly IConfiguration _config;
        private readonly ILogger<PacketPipeline> _logger;

        private ElasticSearchService _elasticSearchService;
        private RedisService _redisService;

        private Task? _redisTask;
        private Task? _elasticTask;
        private Task? _clearingTask;

        private ConcurrentQueue<BasePacketDocument> _packetsQueue;
        private ConcurrentQueue<StatisticsDocument> _statisticsQueue;
        private int _maxQueueSize;

        /// <summary>
        /// Конструктор.
        /// </summary>
        /// <param name="config">Файл конфигурации.</param>
        /// <param name="logger">Логгер.</param>
        public PacketPipeline(IConfiguration config, ILogger<PacketPipeline> logger)
        {
            _config = config;
            _logger = logger;
         
            _redisTask = Task.Run(() => { _redisService = new RedisService(config, logger); });         
            _elasticTask = Task.Run(() => { _elasticSearchService = new ElasticSearchService(config, logger); });

            if (int.TryParse(_config["MaxQueueSize"], out int maxQueueSize))
            {
                _maxQueueSize = maxQueueSize;
            }
            else
            {
                _logger.LogError(Error.FailedToReadMaxQueueSize);
                Environment.Exit(1);
            }

            _packetsQueue = new ConcurrentQueue<BasePacketDocument>();
            _statisticsQueue = new ConcurrentQueue<StatisticsDocument>();
        }

        
        /// <summary>
        /// Входящий метод, получающий список агентов и запускающий прослушивание потоков каждого агента.
        /// </summary>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.WhenAll(_redisTask!, _elasticTask!);

            var agents = _redisService.GetRedisKeys();
            while (!agents.Any())
            {
                try
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    _logger.LogWarning(Warning.NoAgentsWereFound);
                    await Task.Delay(10000);
                    agents = _redisService.GetRedisKeys();
                }
                catch (OperationCanceledException)
                {
                    Environment.Exit(0);
                }                            
            }

            int streamCount = 500;
            if (!int.TryParse(_config["StreamCount"], out streamCount))
            {
                _logger.LogWarning(Warning.FailedToReadStreamCount);
            }
                   
            _clearingTask = Task.Run(() => _redisService.ClearRedisStreamAsync(agents, stoppingToken));

            var tasks = new List<Task>();

            foreach (var agent in agents)
            {
                tasks.Add(Task.Run(async () =>
                {                 
                    var offset = StreamPosition.Beginning;
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var entries = await _redisService.ReadStreamAsync(agent, offset, streamCount);

                            var rawPackets = NetworkHandler.GetDeserializedRawPackets(entries);
                            foreach (var p in rawPackets)
                            {
                                var packet = Packet.ParsePacket((LinkLayers)p.LinkLayerType, p.Data);
                                await GenerateAndIndexNetworkAsync(packet, agent, stoppingToken);
                            }

                            var statistics = NetworkHandler.GetDeserializedStatistics(entries);
                            foreach (var s in statistics)
                            {
                                await GenerateAndIndexStatisticsAsync(s!, agent, stoppingToken);
                            }

                            offset = entries.Last().Id;
                        }
                        catch (RedisConnectionException)
                        {
                            await _redisService.ConnectAsync(_config.GetConnectionString("RedisConnection")!);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(Error.Unexpected, ex.Message);
                            Environment.Exit(1);
                        }
                    }                   
                }));
            }
            
            await Task.WhenAll(tasks);

            _clearingTask!.Dispose();
            _clearingTask = null;
        }      

        /// <summary>
        /// Метод, необходимый для индексации пакетов.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task GenerateAndIndexNetworkAsync(Packet packet, RedisKey agent, CancellationToken stoppingToken)
        {
            object? transport = NetworkHandler.GetTransport(packet);
            object? network = NetworkHandler.GetNetwork(packet);

            if (network == null && transport == null) return;
            
            Guid? transportId = transport == null ? null : Guid.NewGuid();
            Guid? networkId = network == null ? null : Guid.NewGuid();
            
            if (network != null)
                await HandleNetworkAsync(network, (Guid)networkId!, transportId, agent, stoppingToken);
            if (transport != null)
                await HandleTransportAsync(transport, (Guid)transportId!, networkId, agent, stoppingToken);         
        }

        /// <summary>
        /// Метод, необходимый для индексации статистики.
        /// </summary>
        /// <param name="statistics">Статистика.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task GenerateAndIndexStatisticsAsync(Statistics statistics, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = NetworkHandler.GenerateStatisticsDocument(statistics, agent.ToString());

            if (_statisticsQueue.Count < _maxQueueSize)
            {
                _statisticsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("statistics");

                while (_statisticsQueue.TryDequeue(out var d))
                {
                    bulkDescriptor.Index<StatisticsDocument>(s => s
                        .Document(d)
                        .Id(d.Id)
                    );
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с IPv4 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="ipv4"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task HandleNetworkAsync(object network, Guid networkId, Guid? transportId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = NetworkHandler.GenerateNetworkDocument(network, networkId, transportId, agent.ToString());

            if (document == null) return;
            
            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                while (_packetsQueue.TryDequeue(out var d))
                {
                    if (d is IPv4Document)
                    {
                        var doc = (IPv4Document)d;
                        bulkDescriptor.Index<IPv4Document>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("ipv4")
                        );
                    }
                    else if (d is IPv6Document)
                    {
                        var doc = (IPv6Document)d;
                        bulkDescriptor.Index<IPv6Document>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("ipv6")
                        );
                    }
                    else if (d is IcmpV4Document)
                    {
                        var doc = (IcmpV4Document)d;
                        bulkDescriptor.Index<IcmpV4Document>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("icmpv4")
                        );
                    }
                    else if (d is IcmpV6Document)
                    {
                        var doc = (IcmpV6Document)d;
                        bulkDescriptor.Index<IcmpV6Document>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("icmpv6")
                        );
                    }
                    else if (d is IgmpV2Document)
                    {
                        var doc = (IgmpV2Document)d;
                        bulkDescriptor.Index<IgmpV2Document>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("igmp")
                        );
                    }
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с Tcp пакетом.
        /// </summary>
        /// <param name="transportId"></param>
        /// <param name="networkId"></param>
        /// <param name="agent"></param>
        /// <param name="tcp"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task HandleTransportAsync(object transport, Guid transportId, Guid? networkId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = NetworkHandler.GenerateTransportDocument(transport, transportId, networkId, agent.ToString());

            if (document == null) return;

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                while (_packetsQueue.TryDequeue(out var d))
                {
                    if (d is TcpDocument)
                    {
                        var doc = (TcpDocument)d;
                        bulkDescriptor.Index<TcpDocument>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("tcp")
                        );
                    }  
                    else if (d is UdpDocument)
                    {
                        var doc = (UdpDocument)d;
                        bulkDescriptor.Index<UdpDocument>(s => s
                            .Document(doc)
                            .Id(doc.Id)
                            .Index("udp")
                        );
                    }
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);
            }
        }
    }
}
