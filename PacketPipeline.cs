using Nest;
using PacketDataIndexer.Resources;
using StackExchange.Redis;

namespace PacketDataIndexer
{
    internal class PacketPipeline : BackgroundService
    {
        private IDatabase _redisDatabase;
        private ConnectionMultiplexer _redisConnection;
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
                _logger.LogError(Error.FailedToReadElasticConnectionString);
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
            foreach (var agent in agents)
            {
                
            }

            await _redisConnection.CloseAsync();

            _redisTask!.Dispose();
            _redisTask = null;
            _elasticTask!.Dispose();
            _elasticTask = null;
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
    }
}
