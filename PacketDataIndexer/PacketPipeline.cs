using Nest;
using PacketDataIndexer.Models;
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
        private readonly PerfomanceCalculator _perfomanceCalculator;

        private Task? _redisTask;
        private Task? _elasticTask;
        private Task? _clearingTask;

        private List<AgentPackets> _packets;
        private List<AgentStatistics> _statistics;
        private List<AgentMetrics> _metrics;

        private int _maxListSize;
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
            _perfomanceCalculator = new PerfomanceCalculator();

            _packets = new List<AgentPackets>(_maxListSize);
            _statistics = new List<AgentStatistics>(_maxListSize);
            _metrics = new List<AgentMetrics>(_maxListSize);
        }

        /// <summary>
        /// Проверка файла конфигурации.
        /// </summary>
        private void CheckConfiguration()
        {
            if (int.TryParse(_config["MaxQueueSize"], out int maxQueueSize))
            {
                _maxListSize = maxQueueSize;
            }
            else
            {
                _maxListSize = 50;
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
                await _elasticSearchService.ConnectAsync(_elasticConnectionString!, _elasticUsername, _elasticPassword, _elasticConnectionDelay));
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
                    await Task.Delay(TimeSpan.FromSeconds(_redisAgentsReadDelay), stoppingToken);
                    agents = _redisService.GetRedisKeys(_config.GetConnectionString("RedisConnection")!, _redisPort);
                }
                catch (OperationCanceledException)
                {
                    Environment.Exit(0);
                }
            }

            foreach (var agent in agents)
            {
                _packets.Add(new AgentPackets
                {
                    Agent = agent.ToString(),
                    Packets = []
                });
                _statistics.Add(new AgentStatistics
                {
                    Agent = agent.ToString(),
                    Statistics = []
                });
                _metrics.Add(new AgentMetrics
                {
                    Agent = agent.ToString(),
                    Metrics = []
                });
            }

            _clearingTask = Task.Run(async () => await _redisService.ClearRedisStreamAsync(_streamClearTimeout, _streamTTL, agents, stoppingToken), stoppingToken);

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
                                await Task.Delay(TimeSpan.FromSeconds(_streamReadDelay), stoppingToken);
                                entries = await _redisService.ReadStreamAsync(agent, offset, _streamCount);
                            }

                            var stringAgent = agent.ToString();

                            var rawPackets = Deserializer.GetDeserializedRawPackets(entries);
                            if (rawPackets.Count == 0)
                            {
                                await Task.Delay(TimeSpan.FromSeconds(_streamReadDelay), stoppingToken);
                                continue;
                            }

                            var statistics = Deserializer.GetDeserializedStatistics(entries);

                            var rpTask = Task.Run(async () =>
                            {
                                foreach (var rp in rawPackets)
                                {
                                    var packet = Packet.ParsePacket((LinkLayers)rp!.LinkLayerType, rp.Data);
                                    await CalculateAndIndexMetricsAsync(rp.Timeval, packet, stringAgent, stoppingToken);
                                    await ExtractAndHandlePacketAsync(rp.Timeval, packet, stringAgent, stoppingToken);
                                }
                            });

                            var statTask = Task.Run(async () =>
                            {
                                foreach (var s in statistics)
                                {
                                    await GenerateAndIndexStatisticsAsync(s!, stringAgent, stoppingToken);
                                }
                            });

                            await Task.WhenAll(rpTask, statTask);

                            offset = entries.Last().Id;
                        }
                        catch (RedisConnectionException)
                        {
                            await _redisService.ConnectAsync(_config.GetConnectionString("RedisConnection")!);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(Error.Unexpected, ex.Message);
                            await Task.Delay(TimeSpan.FromSeconds(20));
                        }
                    }
                }, stoppingToken));
            }

            await Task.WhenAll(tasks);

            _clearingTask!.Dispose();
            _clearingTask = null;
        }

        /// <summary>
        /// Метод, необходимый для вычисления и индексации метрик сети.
        /// </summary>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task CalculateAndIndexMetricsAsync(Timeval timeval, Packet packet, string agent, CancellationToken stoppingToken)
        {
            var currentMetrics = _perfomanceCalculator.GetCurrentMetrics(packet, timeval);
                
            if (currentMetrics != null)
            {
                var document = DocumentGenerator.GeneratePcapMetricsDocument(currentMetrics, agent);

                var currentAgentMetrics = _metrics.First(m => m.Agent == agent);
                if (currentAgentMetrics.Metrics.Count < _maxListSize)
                {
                    currentAgentMetrics.Metrics.Add(document);
                }
                else
                {
                    var bulkDescriptor = new BulkDescriptor("pcap_metrics");

                    foreach (var metrics in currentAgentMetrics.Metrics)
                    {
                        Indexator.IndexPcapMetrics(bulkDescriptor, metrics);
                    }

                    await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);

                    currentAgentMetrics.Metrics.Clear();
                }
            }
        }

        /// <summary>
        /// Метод, необходимый для извлечения и обработки пакетов.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task ExtractAndHandlePacketAsync(Timeval timeval, Packet packet, string agent, CancellationToken stoppingToken)
        {
            var internet = PacketExtractor.ExtractInternet(packet);

            if (internet == null) 
                return;

            await HandleInternetAsync(
                timeval: timeval,
                internet: internet,
                agent: agent, 
                stoppingToken: stoppingToken);
        }

        /// <summary>
        /// Метод, необходимый для индексации статистики.
        /// </summary>
        /// <param name="statistics">Статистика.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task GenerateAndIndexStatisticsAsync(Statistics statistics, string agent, CancellationToken stoppingToken)
        {
            var document = DocumentGenerator.GenerateStatisticsDocument(statistics, agent.ToString());

            var currentAgentStats = _statistics.First(s => s.Agent == agent);
            if (currentAgentStats.Statistics.Count < _maxListSize)
            {
                currentAgentStats.Statistics.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("statistics");

                foreach (var stat in currentAgentStats.Statistics)
                {
                    Indexator.IndexStatistics(bulkDescriptor, stat);
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);

                currentAgentStats.Statistics.Clear();
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с пакетом сетевого уровня.
        /// </summary>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleInternetAsync(Timeval timeval, object internet, string agent, CancellationToken stoppingToken)
        {
            var document = DocumentGenerator.GenerateInternetDocument(timeval, internet, agent.ToString());
            if (document == null) 
                return;

            var currentAgentPackets = _packets.First(p => p.Agent == agent);
            if (currentAgentPackets.Packets.Count < _maxListSize)
            {
                currentAgentPackets.Packets.Add(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor();

                foreach (var packet in currentAgentPackets.Packets)
                {
                    if (packet is IPv4Document ipv4)                   
                        Indexator.IndexIPv4(bulkDescriptor, ipv4);
                }

                await _elasticSearchService.BulkAsync(bulkDescriptor, stoppingToken);

                currentAgentPackets.Packets.Clear();
            }
        }
    }
}
