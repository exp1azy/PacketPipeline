﻿using Nest;
using PacketDataIndexer.Resources;
using PacketDataIndexer.Services;
using PacketDotNet;
using StackExchange.Redis;
using WebSpectre.Shared;
using WebSpectre.Shared.ES;
using WebSpectre.Shared.Services;
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

        private readonly ElasticSearchService _elasticSearchService;
        private readonly RedisService _redisService;

        private Task? _redisTask;
        private Task? _elasticTask;
        private Task? _clearingTask;

        private List<BasePacketDocument> _packetsQueue;
        private List<StatisticsDocument> _statisticsQueue;

        private int _maxQueueSize;
        private int _streamClearTimeout;
        private int _streamTTL;
        private string? _redisConnectionString;
        private int _redisConnectionDelay;
        private int _redisPort;
        private int _redisAgentsReadDelay;
        private int _streamCount;
        private int _streamReadDelay;
        private string? _elasticConnectionString;
        private string? _elasticUsername;
        private string? _elasticPassword;
        private int _elasticConnectionDelay;
        
        /// <summary>
        /// Конструктор.
        /// </summary>
        /// <param name="config">Файл конфигурации.</param>
        /// <param name="logger">Логгер.</param>
        public PacketPipeline(IConfiguration config, ILogger<PacketPipeline> logger)
        {
            _config = config;
            _logger = logger;

            _redisService = new RedisService(_logger);
            _elasticSearchService = new ElasticSearchService(_logger);

            _packetsQueue = new List<BasePacketDocument>(_maxQueueSize);
            _statisticsQueue = new List<StatisticsDocument>(_maxQueueSize);
        }

        /// <summary>
        /// Проверка файла конфигурации.
        /// </summary>
        private void CheckConfiguration()
        {
            if (int.TryParse(_config["MaxQueueSize"], out int maxQueueSize))
            {
                _maxQueueSize = maxQueueSize;
            }
            else
            {
                _maxQueueSize = 50;
                _logger.LogWarning(Warning.FailedToReadMaxQueueSize);
            }

            _redisConnectionString = _config.GetConnectionString("RedisConnection");
            if (string.IsNullOrEmpty(_redisConnectionString))
            {
                _logger.LogError(Error.FailedToReadRedisConnectionString);
                Environment.Exit(1);
            }

            _elasticConnectionString = _config.GetConnectionString("ElasticConnection");
            if (string.IsNullOrEmpty(_elasticConnectionString))
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
            _elasticUsername = authParams["Username"];
            _elasticPassword = authParams["Password"];

            if (!int.TryParse(_config["RedisConnectionDelay"], out _redisConnectionDelay))
            {
                _redisConnectionDelay = 10;
                _logger.LogWarning(Warning.FailedToReadRedisConnectionDelay);
            }

            if (!int.TryParse(_config["ElasticConnectionDelay"], out _elasticConnectionDelay))
            {
                _elasticConnectionDelay = 10;
                _logger.LogWarning(Warning.FailedToReadElasticConnectionDelay);
            }

            if (!int.TryParse(_config["RedisPort"], out _redisPort))
            {
                _logger.LogError(Error.FailedToReadRedisPort);
                Environment.Exit(1);
            }

            if (!int.TryParse(_config["AgentsReadDelay"], out _redisAgentsReadDelay))
            {
                _redisAgentsReadDelay = 10;
                _logger.LogWarning(Warning.FailedToReadAgentsReadDelay);
            }

            if (!int.TryParse(_config["ClearTimeout"], out _streamClearTimeout))
            {
                _streamClearTimeout = 60;
                _logger.LogWarning(Warning.FailedToReadClearTimeout);
            }

            if (!int.TryParse(_config["StreamTTL"], out _streamTTL))
            {
                _streamTTL = 12;
                _logger.LogWarning(Warning.FailedToReadStreamTTL);
            }

            if (!int.TryParse(_config["StreamCount"], out _streamCount))
            {
                _streamCount = 500;
                _logger.LogWarning(Warning.FailedToReadStreamCount);
            }

            if (!int.TryParse(_config["StreamReadDelay"], out _streamReadDelay))
            {
                _streamReadDelay = 10;
                _logger.LogWarning(Warning.FailedToReadStreamReadDelay);
            }
        }

        /// <summary>
        /// Запуск подключения к серверам ElasticSearch и Redis.
        /// </summary>
        private void StartConnectingToServers()
        {
            _redisTask = Task.Run(async () =>
                await _redisService.ConnectAsync(_redisConnectionString!, _redisConnectionDelay));

            _elasticTask = Task.Run(async () =>
                await _elasticSearchService.ConnectAsync(_elasticConnectionString!, _elasticUsername!, _elasticPassword!, _elasticConnectionDelay));
        }

        /// <summary>
        /// Входящий метод, получающий список агентов и запускающий прослушивание потоков каждого агента.
        /// </summary>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            CheckConfiguration();
            StartConnectingToServers();

            await Task.WhenAll(_redisTask!, _elasticTask!);

            var agents = _redisService.GetRedisKeys(_config.GetConnectionString("RedisConnection")!, _redisPort);
            while (!agents.Any())
            { 
                try
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    _logger.LogWarning(Warning.NoAgentsWereFound);
                    await Task.Delay(TimeSpan.FromSeconds(_redisAgentsReadDelay));
                    agents = _redisService.GetRedisKeys(_config.GetConnectionString("RedisConnection")!, _redisPort);
                }
                catch (OperationCanceledException)
                {
                    Environment.Exit(0);
                }
            }

            _clearingTask = Task.Run(async () => await _redisService.ClearRedisStreamAsync(_streamClearTimeout, _streamTTL, agents, stoppingToken));

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
                            var entries = await _redisService.ReadStreamAsync(agent, offset, _streamCount);
                            while (entries.Length == 0)
                            {
                                _logger.LogWarning(Warning.StreamIsEmpty, agent);
                                await Task.Delay(TimeSpan.FromSeconds(_streamReadDelay));
                                entries = await _redisService.ReadStreamAsync(agent, offset, _streamCount);
                            }

                            var rawPackets = Deserializer.GetDeserializedRawPackets(entries);
                            foreach (var rp in rawPackets)
                            {
                                var packet = Packet.ParsePacket((LinkLayers)rp!.LinkLayerType, rp.Data);
                                await GenerateAndIndexNetworkAsync(packet, agent, stoppingToken);
                            }

                            var statistics = Deserializer.GetDeserializedStatistics(entries);
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
            var transport = PacketExtractor.ExtractTransport(packet);
            var internet = PacketExtractor.ExtractInternet(packet);

            if (internet == null && transport == null) return;

            Guid? transportId = transport == null ? null : Guid.NewGuid();
            Guid? internetId = internet == null ? null : Guid.NewGuid();

            if (internet != null)
                await HandleInternetAsync(internet, (Guid)internetId!, transportId, agent, stoppingToken);
            if (transport != null)
                await HandleTransportAsync(transport, (Guid)transportId!, internetId, agent, stoppingToken);
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
            var document = StatisticsGenerator.GenerateStatisticsDocument(statistics, agent.ToString());

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
        /// <param name="internetId">Идентификатор пакета сетевого уровня.</param>
        /// <param name="transportId">Идентификатор пакета транспортного уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleInternetAsync(object internet, Guid internetId, Guid? transportId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = DocumentGenerator.GenerateInternetDocument(internet, internetId, transportId, agent.ToString());
            if (document == null) return;

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                foreach (var d in _packetsQueue.Where(p => p.Model == OSIModel.Internet.ToString()))
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

                _packetsQueue.RemoveAll(p => p.Model == OSIModel.Internet.ToString());
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с пакетом траспортного уровня.
        /// </summary>
        /// <param name="transport">Пакет транспортного уровня.</param>
        /// <param name="transportId">Идентификатор пакета транспортного уровня.</param>
        /// <param name="internetId">Идентификатор пакеты сетевого уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleTransportAsync(object transport, Guid transportId, Guid? internetId, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = DocumentGenerator.GenerateTransportDocument(transport, transportId, internetId, agent.ToString());
            if (document == null) return;

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                foreach (var d in _packetsQueue.Where(p => p.Model == OSIModel.Transport.ToString()))
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

                _packetsQueue.RemoveAll(p => p.Model == OSIModel.Transport.ToString());
            }
        }
    }
}
