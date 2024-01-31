using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.ES;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;
using System.Collections.Concurrent;
using Error = PacketDataIndexer.Resources.Error;
using IPv6ExtensionHeader = PacketDataIndexer.Entities.ES.IPv6ExtensionHeader;

namespace PacketDataIndexer
{
    /// <summary>
    /// Конвейер пакетов.
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

            _packetsQueue = new ConcurrentQueue<BasePacketDocument>();
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
                                await HandleStatisticsAsync(s!, agent, stoppingToken);
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
            object? transport = GetTransport(packet);
            object? network = GetNetwork(packet);

            if (network == null && transport == null) return;
            
            Guid? transportId = transport == null ? null : Guid.NewGuid();
            Guid? networkId = network == null ? null : Guid.NewGuid();
            
            if (network != null)
            {
                if (network is IPv4Packet)
                {
                    IPv4Packet ipv4 = (IPv4Packet)network;
                    await GenerateAndIndexIPv4DocAsync((Guid)networkId!, transportId, agent, ipv4, stoppingToken);
                }
                if (network is IPv6Packet)
                {
                    IPv6Packet ipv6 = (IPv6Packet)network;
                    await GenerateAndIndexIPv6DocAsync((Guid)networkId!, transportId, agent, ipv6, stoppingToken);
                }
                if (network is IcmpV4Packet)
                {
                    IcmpV4Packet icmpv4 = (IcmpV4Packet)network;
                    await GenerateAndIndexIcmpV4DocAsync((Guid)networkId!, transportId, agent, icmpv4, stoppingToken);
                }
                if (network is IcmpV6Packet)
                {
                    IcmpV6Packet icmpv6 = (IcmpV6Packet)network;
                    await GenerateAndIndexIcmpV6DocAsync((Guid)networkId!, transportId, agent, icmpv6, stoppingToken);
                }
                if (network is IgmpV2Packet)
                {
                    IgmpV2Packet igmp = (IgmpV2Packet)network;
                    await GenerateAndIndexIgmpV2DocAsync((Guid)networkId!, transportId, agent, igmp, stoppingToken);
                }
            }
            if (transport != null)
            {
                if (transport is TcpPacket)
                {
                    TcpPacket tcp = (TcpPacket)transport;
                    await GenerateAndIndexTcpDocAsync((Guid)networkId!, transportId, agent, tcp, stoppingToken);
                }
                if (transport is UdpPacket)
                {
                    UdpPacket udp = (UdpPacket)transport;
                    await GenerateAndIndexUdpDocAsync((Guid)networkId!, transportId, agent, udp, stoppingToken);
                }
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
                //var bulkDescriptor = new BulkDescriptor("statistics");

                //while (_statisticsQueue.TryDequeue(out var d))
                //{
                //    bulkDescriptor.Index<StatisticsDocument>(s => s
                //        .Document(d)
                //        .Id(d.Id)
                //    );
                //}

                //var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                //if (!bulkResponse.IsValid)
                //    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Метод очистки потоков Redis от устаревших данных.
        /// </summary>
        /// <param name="agents">Список агентов.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        private async Task ClearRedisStreamAsync(List<RedisKey> agents, CancellationToken stoppingToken)
        {
            if (!int.TryParse(_config["ClearTimeout"], out int timeout))
            {
                Dispose();
                _logger.LogError(Error.FailedToReadClearTimeout);
                Environment.Exit(1);
            }
            if (!int.TryParse(_config["StreamTTL"], out int ttl))
            {
                Dispose();
                _logger.LogError(Error.FailedToReadStreamTTL);
                Environment.Exit(1);
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(timeout));

                foreach (var agent in agents)
                {
                    var streamInfo = await _redisDatabase.StreamInfoAsync(agent);
                    var entries = await _redisDatabase.StreamReadAsync(agent, streamInfo.FirstEntry.Id);

                     var rawPacketsToDelete = entries
                        .Where(e => e.Values.First().Name.StartsWith("raw_packets"))
                        .Where(e => JsonConvert.DeserializeObject<RawPacket>(e.Values.First().Value.ToString())!.Timeval.Date + TimeSpan.FromHours(ttl) < DateTime.UtcNow)
                        .Select(e => e.Id)
                        .ToArray();
                    if (rawPacketsToDelete.Any())
                        _ = _redisDatabase.StreamDeleteAsync(agent, rawPacketsToDelete);

                    var statisticsToDelete = entries
                        .Where(e => e.Values.First().Name.StartsWith("statistics"))
                        .Where(e => JsonConvert.DeserializeObject<Statistics>(e.Values.First().Value.ToString())!.Timeval.Date + TimeSpan.FromHours(ttl) < DateTime.UtcNow)
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

        /// <summary>
        /// Метод, извлекающий пакет транспортного уровня модели OSI.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет транспортного уровня.</returns>
        private object? GetTransport(Packet packet) =>
            packet.Extract<TcpPacket>() ?? (object)packet.Extract<UdpPacket>();

        /// <summary>
        /// Метод, извлекающий пакет сетевого уровня модели OSI.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет сетевого уровня.</returns>
        private object? GetNetwork(Packet packet) =>
            packet.Extract<IcmpV4Packet>() ?? packet.Extract<IcmpV6Packet>() ??
            packet.Extract<IgmpV2Packet>() ?? packet.Extract<IPv4Packet>() ?? (object)packet.Extract<IPv6Packet>();

        /// <summary>
        /// Фомирование и индексация документа с IPv4 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="ipv4"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexIPv4DocAsync(Guid networkId, Guid? transportId, RedisKey agent, IPv4Packet ipv4, CancellationToken stoppingToken)
        {
            var document = new IPv4Document
            {
                Id = networkId,
                Nested = transportId,
                Agent = agent.ToString(),
                Bytes = ipv4.Bytes,
                HasPayloadData = ipv4.HasPayloadData,
                HasPayloadPacket = ipv4.HasPayloadPacket,
                HeaderData = ipv4.HeaderData,
                PayloadData = ipv4.PayloadData,
                IsPayloadInitialized = ipv4.IsPayloadInitialized,
                TotalPacketLength = ipv4.TotalPacketLength,
                Checksum = ipv4.Checksum,
                Color = ipv4.Color,
                DestinationAddress = ipv4.DestinationAddress,
                DifferentiatedServices = ipv4.DifferentiatedServices,
                FragmentFlags = ipv4.FragmentFlags,
                FragmentOffset = ipv4.FragmentOffset,
                HeaderLength = ipv4.HeaderLength,
                HopLimit = ipv4.HopLimit,
                IPv4Id = ipv4.Id,
                PayloadLength = ipv4.PayloadLength,
                Protocol = ipv4.Protocol,
                SourceAddress = ipv4.SourceAddress,
                TimeToLive = ipv4.TimeToLive,
                TotalLength = ipv4.TotalLength,
                TypeOfService = ipv4.TypeOfService,
                ValidChecksum = ipv4.ValidChecksum,
                ValidIPChecksum = ipv4.ValidIPChecksum,
                Version = ipv4.Version
            };

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("ipv4");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (IPv4Document)d;
                    bulkDescriptor.Index<IPv4Document>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с IPv6 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="ipv6"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexIPv6DocAsync(Guid networkId, Guid? transportId, RedisKey agent, IPv6Packet ipv6, CancellationToken stoppingToken)
        {
            var document = new IPv6Document
            {
                Id = networkId,
                Nested = transportId,
                Agent = agent.ToString(),
                Bytes = ipv6.Bytes,
                HasPayloadData = ipv6.HasPayloadData,
                HasPayloadPacket = ipv6.HasPayloadPacket,
                HeaderData = ipv6.HeaderData,
                PayloadData = ipv6.PayloadData,
                IsPayloadInitialized = ipv6.IsPayloadInitialized,
                TotalPacketLength = ipv6.TotalPacketLength,
                Color = ipv6.Color,
                DestinationAddress = ipv6.DestinationAddress,
                ExtensionHeaders = ipv6.ExtensionHeaders.Select(h => (IPv6ExtensionHeader)h).ToList(),
                ExtensionHeadersLength = ipv6.ExtensionHeadersLength,
                FlowLabel = ipv6.FlowLabel,
                HeaderLength = ipv6.HeaderLength,
                HopLimit = ipv6.HopLimit,
                NextHeader = ipv6.NextHeader,
                PayloadLength = ipv6.PayloadLength,
                Protocol = ipv6.Protocol,
                SourceAddress = ipv6.SourceAddress,
                TimeToLive = ipv6.TimeToLive,
                TotalLength = ipv6.TotalLength,
                TrafficClass = ipv6.TrafficClass,
                Version = ipv6.Version
            };

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("ipv6");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (IPv6Document)d;
                    bulkDescriptor.Index<IPv6Document>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с IcmpV4 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="icmpv4"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexIcmpV4DocAsync(Guid networkId, Guid? transportId, RedisKey agent, IcmpV4Packet icmpv4, CancellationToken stoppingToken)
        {
            var document = new IcmpV4Document
            {
                Id = networkId,
                Nested = transportId,
                Agent = agent.ToString(),
                Bytes = icmpv4.Bytes,
                HasPayloadData = icmpv4.HasPayloadData,
                HeaderData = icmpv4.HeaderData,
                HasPayloadPacket = icmpv4.HasPayloadPacket,
                IsPayloadInitialized = icmpv4.IsPayloadInitialized,
                PayloadData = icmpv4.PayloadData,
                TotalPacketLength = icmpv4.TotalPacketLength,
                Color = icmpv4.Color,
                Checksum = icmpv4.Checksum,
                TypeCode = icmpv4.TypeCode,
                Data = icmpv4.Data,
                IcmpV4Id = icmpv4.Id,
                Sequence = icmpv4.Sequence,
                ValidIcmpChecksum = icmpv4.ValidIcmpChecksum
            };

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("icmp4");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (IcmpV4Document)d;
                    bulkDescriptor.Index<IcmpV4Document>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с IcmpV6 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="icmpv6"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexIcmpV6DocAsync(Guid networkId, Guid? transportId, RedisKey agent, IcmpV6Packet icmpv6, CancellationToken stoppingToken)
        {
            var document = new IcmpV6Document
            {
                Id = networkId,
                Nested = transportId,
                Agent = agent.ToString(),
                Bytes = icmpv6.Bytes,
                HasPayloadData = icmpv6.HasPayloadData,
                HasPayloadPacket = icmpv6.HasPayloadPacket,
                HeaderData = icmpv6.HeaderData,
                IsPayloadInitialized = icmpv6.IsPayloadInitialized,
                PayloadData = icmpv6.PayloadData,
                TotalPacketLength = icmpv6.TotalPacketLength,
                Color = icmpv6.Color,
                Checksum = icmpv6.Checksum,
                Code = icmpv6.Code,
                ValidIcmpChecksum = icmpv6.ValidIcmpChecksum,
                Type = icmpv6.Type
            };

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("icmp6");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (IcmpV6Document)d;
                    bulkDescriptor.Index<IcmpV6Document>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с IgmpV2 пакетом.
        /// </summary>
        /// <param name="networkId"></param>
        /// <param name="transportId"></param>
        /// <param name="agent"></param>
        /// <param name="igmp"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexIgmpV2DocAsync(Guid networkId, Guid? transportId, RedisKey agent, IgmpV2Packet igmp, CancellationToken stoppingToken)
        {
            var document = new IgmpV2Document
            {
                Id = networkId,
                Nested = transportId,
                Agent = agent.ToString(),
                Bytes = igmp.Bytes,
                Checksum = igmp.Checksum,
                Color = igmp.Color,
                GroupAddress = igmp.GroupAddress,
                HasPayloadData = igmp.HasPayloadData,
                HasPayloadPacket = igmp.HasPayloadPacket,
                HeaderData = igmp.HeaderData,
                IsPayloadInitialized = igmp.IsPayloadInitialized,
                MaxResponseTime = igmp.MaxResponseTime,
                PayloadData = igmp.PayloadData,
                TotalPacketLength = igmp.TotalPacketLength,
                Type = igmp.Type
            };

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("igmp");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (IgmpV2Document)d;
                    bulkDescriptor.Index<IgmpV2Document>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
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
        private async Task GenerateAndIndexTcpDocAsync(Guid transportId, Guid? networkId, RedisKey agent, TcpPacket tcp, CancellationToken stoppingToken)
        {
            var document = new TcpDocument
            {
                Id = transportId,
                Nested = networkId,
                Agent = agent.ToString(),
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
                OptionsCollection = tcp.OptionsCollection.Select(o => (TcpOption)o).ToList(),
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

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("tcp");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (TcpDocument)d;
                    bulkDescriptor.Index<TcpDocument>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
        }

        /// <summary>
        /// Фомирование и индексация документа с Udp пакетом.
        /// </summary>
        /// <param name="transportId"></param>
        /// <param name="networkId"></param>
        /// <param name="agent"></param>
        /// <param name="udp"></param>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task GenerateAndIndexUdpDocAsync(Guid transportId, Guid? networkId, RedisKey agent, UdpPacket udp, CancellationToken stoppingToken)
        {
            var document = new UdpDocument
            {
                Id = transportId,
                Nested = networkId,
                Agent = agent.ToString(),
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

            if (_packetsQueue.Count < _maxQueueSize)
            {
                _packetsQueue.Enqueue(document);
            }
            else
            {
                var bulkDescriptor = new BulkDescriptor("udp");

                while (_packetsQueue.TryDequeue(out var d))
                {
                    var doc = (UdpDocument)d;
                    bulkDescriptor.Index<UdpDocument>(s => s
                        .Document(doc)
                        .Id(doc.Id)
                    );
                }

                var bulkResponse = await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);

                if (!bulkResponse.IsValid)
                    _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, bulkResponse.OriginalException.Message);
            }
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
