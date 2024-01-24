using Nest;
using Newtonsoft.Json;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.Deserialized.RawPacket;
using PacketDataIndexer.Entities.Deserialized.Statistics;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;

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

        /// <summary>
        /// Конструктор.
        /// </summary>
        /// <param name="config">Файл конфигурации.</param>
        /// <param name="logger">Логгер.</param>
        public PacketPipeline(IConfiguration config, ILogger<PacketPipeline> logger)
        {
            _config = config;
            _logger = logger;

            var connectionString = _config.GetSection("ConnectionStrings");
            if (connectionString["RedisConnection"] == null)
            {
                _logger.LogError(Error.FailedToReadRedisConnectionString);
                Environment.Exit(1);
            }
            _redisTask = Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        _redisConnection = ConnectionMultiplexer.Connect(connectionString["RedisConnection"]!);
                        _redisDatabase = _redisConnection.GetDatabase();
                        break;
                    }
                    catch
                    {
                        _logger.LogError(Error.NoConnectionToRedis);
                        await Task.Delay(2000);
                    }
                }
            });

            //if (connectionString["ElasticClient"] == null)
            //{
            //    _logger?.LogError(Error.FailedToReadElasticConnectionString);
            //    Environment.Exit(1);
            //}
            _elasticTask = Task.Run(async () =>
            {
                //while (true)
                //{
                //    try
                //    {
                //        var settings = new ConnectionSettings(new Uri(connectionString["ElasticConnection"]!));
                //        _elasticClient = new ElasticClient(settings);
                //        break;
                //    }
                //    catch
                //    {
                //        _logger.LogError(Error.NoConnectionToElastic);
                //        await Task.Delay(2000);
                //    }
                //}
            });
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

                    _logger.LogError(Error.NoAgentsWereFound);
                    await Task.Delay(10000);
                    agents = GetRedisKeys();
                }
                catch (OperationCanceledException)
                {
                    Dispose();
                    Environment.Exit(0);
                }                            
            }

            _clearingTask = Task.Run(() => ClearRedisStreamAsync(agents, stoppingToken));

            var tasks = new List<Task>();

            foreach (var agent in agents)
            {
                tasks.Add(Task.Run(async () =>
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var streamInfo = await _redisDatabase.StreamInfoAsync(agent);
                        var entries = await _redisDatabase.StreamReadAsync(agent, streamInfo.FirstEntry.Id);

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
            var rawPackets = allBatches.Where(v => v.Name.StartsWith("raw_packets"));

            var rawPacketBatches = rawPackets.Select(b => JsonConvert.DeserializeObject<RawPacket>(b.Value.ToString())).ToList();       

            return rawPacketBatches;
        }

        /// <summary>
        /// Метод, необходимый для распаковки и десериализации данных о Statistics из Redis.
        /// </summary>
        /// <returns>Список <see cref="Statistics"/></returns>
        private List<Statistics?> GetDeserializedStatisticsData(StreamEntry[] entries)
        {
            var allBatches = entries.Select(e => e.Values.First());
            var statistics = allBatches.Where(v => v.Name.StartsWith("statistics"));

            var statisticsBatches = statistics.Select(b => JsonConvert.DeserializeObject<Statistics>(b.Value.ToString())).ToList();

            return statisticsBatches;
        }

        /// <summary>
        /// Метод, необходимый для извлечения пакетов и передачи его в индекс ES.
        /// </summary>
        /// <param name="packet"></param>
        /// <param name="agent"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task HandlePacketAsync(Packet packet, RedisKey agent, CancellationToken stoppingToken)
        {
            PacketsDocument document;

            var transport = GetTransport(packet);
            var network = GetNetwork(packet);

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

            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="statistics"></param>
        /// <param name="agent"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task HandleStatisticsAsync(Statistics statistics, RedisKey agent, CancellationToken stoppingToken)
        {
            var document = new StatisticsDocument
            {
                Id = Guid.NewGuid(),
                Agent = agent.ToString(),
                Statistics = statistics
            };
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
        /// Метод, получающий агентов из сервера Redis.
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
