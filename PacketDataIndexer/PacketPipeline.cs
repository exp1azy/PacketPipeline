using Nest;
using Newtonsoft.Json;
using PacketDataIndexer.Entities;
using PacketDataIndexer.Entities.Deserialized;
using PacketDataIndexer.Entities.Deserialized.RawPacket;
using PacketDataIndexer.Entities.Deserialized.Statistics;
using PacketDataIndexer.Resources;
using PacketDotNet;
using StackExchange.Redis;
using System.Net.Sockets;

namespace PacketDataIndexer
{
    internal class PacketPipeline : BackgroundService
    {
        private IDatabase _redisDatabase;
        private ConnectionMultiplexer? _redisConnection;
        private ElasticClient _elasticClient;
        private Task? _redisTask;
        private Task? _elasticTask;

        private readonly IConfiguration _config;
        private readonly ILogger<PacketPipeline> _logger;

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
            
            var tasks = new List<Task>();

            foreach (var agent in agents)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var streamInfo = await _redisDatabase.StreamInfoAsync(agent);                  
                    var entries = await _redisDatabase.StreamReadAsync(agent, streamInfo.FirstEntry.Id);

                    var allBatches = entries.Select(e => e.Values.First());
                    var statisticsBatches = allBatches.Where(v => v.Name.StartsWith("statistics"));
                    var rawPacketBatches = allBatches.Where(v => v.Name.StartsWith("raw_packets"));

                    var statisticsData = statisticsBatches.Select(b => new NetworkData<Statistics>
                    {
                        Batch = JsonConvert.DeserializeObject<List<Statistics>>(b.Value.ToString())!
                    }).ToList();
                    var rawPacketData = rawPacketBatches.Select(b => new NetworkData<RawPacket>
                    {
                        Batch = JsonConvert.DeserializeObject<List<RawPacket>>(b.Value.ToString())!
                    }).ToList();

                    rawPacketData.ForEach(async rp =>
                    {
                        foreach (var p in rp.Batch)
                        {
                            var packet = Packet.ParsePacket((LinkLayers)p.LinkLayerType, p.Data);
                            await HandlePacketAsync(packet, agent, stoppingToken);
                        }
                    });
                }));
            }
            
            await Task.WhenAll(tasks);
        }

        private async Task HandlePacketAsync(Packet packet, RedisKey agent, CancellationToken stoppingToken)
        {
            dynamic transport = packet.Extract<TcpPacket>() ?? (dynamic)packet.Extract<UdpPacket>();
            dynamic network = packet.Extract<IcmpV4Packet>() ?? packet.Extract<IcmpV6Packet>() ??
                packet.Extract<IgmpV2Packet>() ?? packet.Extract<IPv4Packet>() ?? (dynamic)packet.Extract<IPv6Packet>();

            var document = new Document
            {
                
            };
        }

        private List<RedisKey> GetRedisKeys()
        {
            IServer? server = default;
            var agents = new List<RedisKey>();

            try
            {
                server = _redisConnection.GetServer(_config["ConnectionStrings:RedisConnection"]!, 6379);
            }
            catch
            {
                _logger.LogError(Error.NoConnectionToRedisServer);
                Environment.Exit(1);
            }

            foreach (var key in server!.Keys(pattern: "host_*"))
            {
                agents.Add(new RedisKey(key));
            }
            
            return agents;
        }

        public override void Dispose()
        {
            base.Dispose();

            _redisConnection.Close();
            _redisConnection.Dispose();
            _redisConnection = null;

            _redisTask!.Dispose();
            _redisTask = null;
            _elasticTask!.Dispose();
            _elasticTask = null;
        }
    }
}
