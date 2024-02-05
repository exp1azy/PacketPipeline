using Nest;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.ES;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;
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

        private List<BasePacketDocument> _packetsQueue;
        private List<StatisticsDocument> _statisticsQueue;
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

            if (int.TryParse(_config["MaxQueueSize"], out int maxQueueSize))
            {
                _maxQueueSize = maxQueueSize;
            }
            else
            {
                _maxQueueSize = 50;
                _logger.LogWarning(Warning.FailedToReadMaxQueueSize);
            }

            _packetsQueue = new List<BasePacketDocument>(_maxQueueSize);
            _statisticsQueue = new List<StatisticsDocument>(_maxQueueSize);
        }

        /// <summary>
        /// Запуск подключения к серверам ElasticSearch и Redis.
        /// </summary>
        private void StartConnectingToServers(int redisConnectionDelay, int elasticConnectionDelay)
        {
            var redisConnection = _config.GetConnectionString("RedisConnection");
            if (string.IsNullOrEmpty(redisConnection))
            {
                _logger.LogError(Error.FailedToReadRedisConnectionString);
                Environment.Exit(1);
            }

            _redisService = new RedisService(_logger);
            _redisTask = Task.Run(async () => 
                await _redisService.ConnectAsync(redisConnection, redisConnectionDelay));

            var elasticConnection = _config.GetConnectionString("ElasticConnection");
            if (string.IsNullOrEmpty(elasticConnection))
            {
                _logger?.LogError(Error.FailedToReadElasticConnectionString);
                Environment.Exit(1);
            }

            var authParams = _config.GetSection("ElasticSearchAuth");
            if (string.IsNullOrEmpty(authParams["Username"]) || string.IsNullOrEmpty(authParams["Password"]))
            {
                _logger.LogError(Error.FailedToReadESAuthParams);
                Environment.Exit(1);
            }

            _elasticSearchService = new ElasticSearchService(_logger);
            _elasticTask = Task.Run(async () => 
                await _elasticSearchService.ConnectAsync(elasticConnection, authParams["Username"]!, authParams["Password"]!, elasticConnectionDelay));
        }

        /// <summary>
        /// Входящий метод, получающий список агентов и запускающий прослушивание потоков каждого агента.
        /// </summary>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            int redisConnectionDelay;
            if (!int.TryParse(_config["RedisConnectionDelay"], out redisConnectionDelay))
            {
                redisConnectionDelay = 10;
                _logger.LogWarning(Warning.FailedToReadRedisConnectionDelay);
            }

            int elasticConnectionDelay;
            if (!int.TryParse(_config["ElasticConnectionDelay"], out elasticConnectionDelay))
            {
                elasticConnectionDelay = 10;
                _logger.LogWarning(Warning.FailedToReadElasticConnectionDelay);
            }

            StartConnectingToServers(redisConnectionDelay, elasticConnectionDelay);

            await Task.WhenAll(_redisTask!, _elasticTask!);

            if (!int.TryParse(_config["RedisPort"], out int port))
            {
                _logger.LogError(Error.FailedToReadRedisPort);
                Environment.Exit(1);
            }

            var agents = _redisService.GetRedisKeys(_config.GetConnectionString("RedisConnection")!, port);
            while (!agents.Any())
            {
                int agentsReadDelay;
                if (!int.TryParse(_config["AgentsReadDelay"], out agentsReadDelay))
                {
                    agentsReadDelay = 10;
                    _logger.LogWarning(Warning.FailedToReadAgentsReadDelay); 
                }

                try
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    _logger.LogWarning(Warning.NoAgentsWereFound);
                    await Task.Delay(TimeSpan.FromSeconds(agentsReadDelay));
                    agents = _redisService.GetRedisKeys(_config.GetConnectionString("RedisConnection")!, 6379);
                }
                catch (OperationCanceledException)
                {
                    Environment.Exit(0);
                }                            
            }

            int timeout;
            if (!int.TryParse(_config["ClearTimeout"], out timeout))
            {
                timeout = 60;
                _logger.LogWarning(Warning.FailedToReadClearTimeout);
            }

            int ttl;
            if (!int.TryParse(_config["StreamTTL"], out ttl))
            {
                ttl = 12;
                _logger.LogWarning(Warning.FailedToReadStreamTTL);
            }

            _clearingTask = Task.Run(async () => await _redisService.ClearRedisStreamAsync(timeout, ttl, agents, stoppingToken));

            int streamCount;
            if (!int.TryParse(_config["StreamCount"], out streamCount))
            {
                streamCount = 500;
                _logger.LogWarning(Warning.FailedToReadStreamCount);
            }

            int streamReadDelay;
            if (!int.TryParse(_config["StreamReadDelay"], out streamReadDelay))
            {
                streamReadDelay = 10;
                _logger.LogWarning(Warning.FailedToReadStreamReadDelay);
            }

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
                            while (entries.Length == 0)
                            {
                                _logger.LogWarning(Warning.StreamIsEmpty, agent);
                                await Task.Delay(TimeSpan.FromSeconds(streamReadDelay));
                                entries = await _redisService.ReadStreamAsync(agent, offset, streamCount);
                            }

                            var rawPackets = NetworkHandler.GetDeserializedRawPackets(entries);
                            foreach (var rp in rawPackets)
                            {
                                var packet = Packet.ParsePacket((LinkLayers)rp!.LinkLayerType, rp.Data);
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
                _statisticsQueue.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("statistics");

                foreach (var d in _statisticsQueue)
                {
                    bulkDescriptor.Index<StatisticsDocument>(s => s
                        .Document(d)
                        .Id(d.Id)
                    );
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);

                _statisticsQueue.Clear();
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с пакетом сетевого уровня.
        /// </summary>
        /// <param name="networkId">Идентификатор пакета сетевого уровня.</param>
        /// <param name="transportId">Идентификатор пакета транспортного уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleNetworkAsync(object network, Guid networkId, Guid? transportId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = NetworkHandler.GenerateNetworkDocument(network, networkId, transportId, agent.ToString());
            if (document == null) return;
            
            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                foreach (var d in _packetsQueue.Where(p => p.Model == "Network"))
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

                _packetsQueue.RemoveAll(p => p.Model == "Network");
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с пакетом траспортного уровня.
        /// </summary>
        /// <param name="transport">Пакет транспортного уровня.</param>
        /// <param name="transportId">Идентификатор пакета транспортного уровня.</param>
        /// <param name="networkId">Идентификатор пакеты сетевого уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleTransportAsync(object transport, Guid transportId, Guid? networkId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = NetworkHandler.GenerateTransportDocument(transport, transportId, networkId, agent.ToString());
            if (document == null) return;

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                foreach (var d in _packetsQueue.Where(p => p.Model == "Transport"))
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

                _packetsQueue.RemoveAll(p => p.Model == "Transport");
            }
        }
    }
}
