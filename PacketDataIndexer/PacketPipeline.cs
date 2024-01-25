﻿using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;
using System.Collections.Concurrent;
using Error = PacketDataIndexer.Resources.Error;

namespace PacketDataIndexer
{
    /// <summary>
    /// Класс, представляющий конвейер пакетов.
    /// </summary>
    internal class PacketPipeline : BackgroundService
    {
        private IDatabase _redisDatabase;
        private ConnectionMultiplexer? _redisConnection;
        private ElasticClient _elasticClient;
        private readonly IConfiguration _config;
        private readonly ILogger<PacketPipeline> _logger;

        private Task? _redisTask;
        private Task? _elasticTask;
        private Task? _clearingTask;

        private ConcurrentQueue<PacketsDocument> _packetsQueue;
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

            var redisConnection = _config.GetConnectionString("RedisConnection");
            if (string.IsNullOrEmpty(redisConnection))
            {
                _logger.LogError(Error.FailedToReadRedisConnectionString);
                Environment.Exit(1);
            }

            _redisTask = Task.Run(() => ConnectToRedisAsync(redisConnection));

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

            _elasticTask = Task.Run(() => ConnectToElasticSearchAsync(
                elasticConnection!,
                authParams["Username"]!, 
                authParams["Password"]!)
            );

            if (int.TryParse(_config["MaxQueueSize"], out int maxQueueSize))
            {
                _maxQueueSize = maxQueueSize;
            }
            else
            {
                _logger.LogError(Error.FailedToReadMaxQueueSize);
                Environment.Exit(1);
            }

            _packetsQueue = new ConcurrentQueue<PacketsDocument>();
            _statisticsQueue = new ConcurrentQueue<StatisticsDocument>();
        }

        /// <summary>
        /// Подключение к серверу Redis.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        /// <returns></returns>
        private async Task ConnectToRedisAsync(string connectionString)
        {         
            while (true)
            {
                try
                {
                    _redisConnection = ConnectionMultiplexer.Connect(connectionString);
                    _redisDatabase = _redisConnection.GetDatabase();
                    break;
                }
                catch
                {
                    _logger.LogError(Error.NoConnectionToRedis);
                    await Task.Delay(5000);
                }
            }
        }

        /// <summary>
        /// Подключение к серверу ElasticSearch.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        /// <param name="username">Имя пользователя.</param>
        /// <param name="password">Пароль.</param>
        /// <returns></returns>
        private async Task ConnectToElasticSearchAsync(string connectionString, string username, string password)
        {
            while (true)
            {
                try
                {
                    var settings = new ConnectionSettings(new Uri(connectionString))
                        .BasicAuthentication(username, password)
                        .ServerCertificateValidationCallback((o, certificate, chain, errors) => true)
                        .ServerCertificateValidationCallback(CertificateValidations.AllowAll)
                        .DisableDirectStreaming();
                    _elasticClient = new ElasticClient(settings);
                    break;
                }
                catch
                {
                    _logger.LogError(Error.NoConnectionToElastic);
                    await Task.Delay(5000);
                }
            }
        }

        /// <summary>
        /// Входящий метод, получающий список агентов и запускающий прослушивание потоков каждого агента.
        /// </summary>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.WhenAll(_redisTask!, _elasticTask!);

            var agents = GetRedisKeys();
            while (!agents.Any())
            {
                try
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    _logger.LogWarning(Warning.NoAgentsWereFound);
                    await Task.Delay(10000);
                    agents = GetRedisKeys();
                }
                catch (OperationCanceledException)
                {
                    Dispose();
                    Environment.Exit(0);
                }                            
            }

            int streamCount = 500;
            if (!int.TryParse(_config["StreamCount"], out streamCount))
            {
                _logger.LogWarning(Warning.FailedToReadStreamCount);
            }
                   
            _clearingTask = Task.Run(() => ClearRedisStreamAsync(agents, stoppingToken));

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
                            var entries = await _redisDatabase.StreamReadAsync(agent, offset, streamCount);

                            var rawPacketData = GetDeserializedRawPacketData(entries);
                            foreach (var p in rawPacketData)
                            {
                                var packet = Packet.ParsePacket((LinkLayers)p.LinkLayerType, p.Data);
                                await HandlePacketAsync(packet, agent, stoppingToken);
                            }

                            var statisticsData = GetDeserializedStatisticsData(entries);
                            foreach (var s in statisticsData)
                            {
                                await HandleStatisticsAsync(s, agent, stoppingToken);
                            }

                            offset = entries.Last().Id;
                        }
                        catch (RedisConnectionException)
                        {
                            await ConnectToRedisAsync(_config.GetConnectionString("RedisConnection")!);
                        }
                        catch (Exception ex)
                        {
                            Dispose();
                            _logger.LogError(Error.Unexpected, ex.Message);
                            Environment.Exit(1);
                        }
                    }                   
                }));
            }
            
            await Task.WhenAll(tasks);

            Dispose();
            _clearingTask!.Dispose();
            _clearingTask = null;
        }

        /// <summary>
        /// Метод, необходимый для распаковки и десериализации данных о RawPacket из Redis.
        /// </summary>
        /// <returns>Список <see cref="RawPacket"/></returns>
        private List<RawPacket?> GetDeserializedRawPacketData(StreamEntry[] entries)
        {
            var allBatches = entries.Select(e => e.Values.First());
            var rawPacketsBatches = allBatches.Where(v => v.Name.StartsWith("raw_packets"));

            var rawPackets = rawPacketsBatches.Select(b => JsonConvert.DeserializeObject<RawPacket>(b.Value.ToString())).ToList();       

            return rawPackets;
        }

        /// <summary>
        /// Метод, необходимый для распаковки и десериализации данных о Statistics из Redis.
        /// </summary>
        /// <returns>Список <see cref="Statistics"/></returns>
        private List<Statistics?> GetDeserializedStatisticsData(StreamEntry[] entries)
        {
            var allBatches = entries.Select(e => e.Values.First());
            var statisticsBatches = allBatches.Where(v => v.Name.StartsWith("statistics"));

            var statistics = statisticsBatches.Select(b => JsonConvert.DeserializeObject<Statistics>(b.Value.ToString())).ToList();

            return statistics;
        }

        /// <summary>
        /// Метод, необходимый для индексации пакетов.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandlePacketAsync(Packet packet, RedisKey agent, CancellationToken stoppingToken)
        {
            PacketsDocument document;

            object? transport = GetTransport(packet);
            object? network = GetNetwork(packet);

            if (network == null && transport == null)
            {
                return;
            }
            else if (transport == null)
            {
                document = new PacketsDocument
                {
                    Id = Guid.NewGuid(),
                    Agent = agent.ToString(),
                    IcmpV4Packet = network.GetType() == typeof(IcmpV4Packet) ? (IcmpV4Packet)network : null,
                    IcmpV6Packet = network.GetType() == typeof(IcmpV6Packet) ? (IcmpV6Packet)network : null,
                    IgmpV2Packet = network.GetType() == typeof(IgmpV2Packet) ? (IgmpV2Packet)network : null,
                    IPv4Packet = network.GetType() == typeof(IPv4Packet) ? (IPv4Packet)network : null,
                    IPv6Packet = network.GetType() == typeof(IPv6Packet) ? (IPv6Packet)network : null,
                };
            }
            else if (network == null)
            {
                document = new PacketsDocument
                {
                    Id = Guid.NewGuid(),
                    Agent = agent.ToString(),
                    TcpPacket = transport.GetType() == typeof(TcpPacket) ? (TcpPacket)transport : null,
                    UdpPacket = transport.GetType() == typeof(UdpPacket) ? (UdpPacket)transport : null
                };
            }
            else
            {
                document = new PacketsDocument
                {
                    Id = Guid.NewGuid(),
                    Agent = agent.ToString(),
                    TcpPacket = transport.GetType() == typeof(TcpPacket) ? (TcpPacket)transport : null,
                    UdpPacket = transport.GetType() == typeof(UdpPacket) ? (UdpPacket)transport : null,
                    IcmpV4Packet = network.GetType() == typeof(IcmpV4Packet) ? (IcmpV4Packet)network : null,
                    IcmpV6Packet = network.GetType() == typeof(IcmpV6Packet) ? (IcmpV6Packet)network : null,
                    IgmpV2Packet = network.GetType() == typeof(IgmpV2Packet) ? (IgmpV2Packet)network : null,
                    IPv4Packet = network.GetType() == typeof(IPv4Packet) ? (IPv4Packet)network : null,
                    IPv6Packet = network.GetType() == typeof(IPv6Packet) ? (IPv6Packet)network : null,
                };
            }

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("packets");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    bulkDescriptor.Index<PacketsDocument>(s => s
                        .Document(d)
                        .Id(d.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Метод, необходимый для индексации статистики.
        /// </summary>
        /// <param name="statistics">Статистика.</param>
        /// <param name="agent">Агент.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task HandleStatisticsAsync(Statistics statistics, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = new StatisticsDocument
            {
                Id = Guid.NewGuid(),
                Agent = agent.ToString(),
                Statistics = statistics
            };

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

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Метод, извлекающий пакет транспортного уровня модели OSI.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет транспортного уровня.</returns>
        private object? GetTransport(Packet packet) => 
            packet.Extract<TcpPacket>() ?? (dynamic)packet.Extract<UdpPacket>();

        /// <summary>
        /// Метод, извлекающий пакет сетевого уровня модели OSI.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет сетевого уровня.</returns>
        private object? GetNetwork(Packet packet) =>
            packet.Extract<IcmpV4Packet>() ?? packet.Extract<IcmpV6Packet>() ??
            packet.Extract<IgmpV2Packet>() ?? packet.Extract<IPv4Packet>() ?? (dynamic)packet.Extract<IPv6Packet>();

        /// <summary>
        /// Метод очистки потоков Redis от устаревших данных.
        /// </summary>
        /// <param name="agents">Список агентов.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task ClearRedisStreamAsync(List<RedisKey> agents, CancellationToken stoppingToken)
        {
            int? timeout = int.Parse(_config["ClearTimeout"]);
            int? ttl = int.Parse(_config["StreamTTL"]);

            if (!timeout.HasValue)
            {
                Dispose();
                _logger.LogError(Error.FailedToReadClearTimeout);
                Environment.Exit(1);
            }
            if (!ttl.HasValue)
            {
                Dispose();
                _logger.LogError(Error.FailedToReadStreamTTL);
                Environment.Exit(1);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(timeout.Value));

                foreach (var agent in agents)
                {
                    var streamInfo = await _redisDatabase.StreamInfoAsync(agent);
                    var entries = await _redisDatabase.StreamReadAsync(agent, streamInfo.FirstEntry.Id);

                     var rawPacketsToDelete = entries
                        .Where(e => e.Values.First().Name.StartsWith("raw_packets"))
                        .Where(e => JsonConvert.DeserializeObject<RawPacket>(e.Values.First().Value.ToString()).Timeval.Date + TimeSpan.FromHours((double)ttl) < DateTime.UtcNow)
                        .Select(e => e.Id)
                        .ToArray();
                    if (rawPacketsToDelete.Any())
                        _ = _redisDatabase.StreamDeleteAsync(agent, rawPacketsToDelete);

                    var statisticsToDelete = entries
                        .Where(e => e.Values.First().Name.StartsWith("statistics"))
                        .Where(e => JsonConvert.DeserializeObject<Statistics>(e.Values.First().Value.ToString()).Timeval.Date + TimeSpan.FromHours((double)ttl) < DateTime.UtcNow)
                        .Select(e => e.Id)  
                        .ToArray();
                    if (statisticsToDelete.Any())
                        _ = _redisDatabase.StreamDeleteAsync(agent, statisticsToDelete);
                }
            }           
        }

        /// <summary>
        /// Метод, возвращающий агентов из сервера Redis.
        /// </summary>
        /// <returns>Список ключей.</returns>
        private List<RedisKey> GetRedisKeys()
        {
            IServer? server = default;
            var agents = new List<RedisKey>();

            try
            {
                server = _redisConnection!.GetServer(_config["ConnectionStrings:RedisConnection"]!, 6379);
            }
            catch
            {
                _logger.LogError(Error.NoConnectionToRedisServer);
                Environment.Exit(1);
            }

            foreach (var key in server!.Keys(pattern: "host_*"))           
                agents.Add(new RedisKey(key));
                       
            return agents;
        }

        public override void Dispose()
        {
            base.Dispose();

            if (_redisConnection!.IsConnected)
                _redisConnection!.Close();

            _redisConnection.Dispose();
            _redisConnection = null;

            _redisTask!.Dispose();
            _redisTask = null;

            _elasticTask!.Dispose();
            _elasticTask = null;
        }
    }
}
