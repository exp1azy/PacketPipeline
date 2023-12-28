using Nest;
using PacketDataIndexer.Resources;
using StackExchange.Redis;
using System.Net;
using System.Net.NetworkInformation;

namespace PacketDataIndexer
{
    internal class PacketPipeline : BackgroundService
    {
        private readonly IDatabase _redisDatabase;
        private readonly ConnectionMultiplexer _redisConnection;
        private readonly ElasticClient _elasticClient;
        private readonly IConfiguration _config;
        private readonly ILogger<PacketPipeline> _logger;

        public PacketPipeline(IConfiguration config, ILogger<PacketPipeline> logger)
        {
            _config = config;
            _logger = logger;

            //var connectionStrings = _config.GetSection("ConnectionStrings");
            //if (connectionStrings["RedisConnection"] == null)
            //{
            //    _logger.LogError(Error.FailedToReadElasticConnectionString);
            //    Environment.Exit(1);
            //}
            //while (true)
            //{
            //    try
            //    {
            //        _redisConnection = ConnectionMultiplexer.Connect(connectionStrings["RedisConnection"]!);
            //        _redisDatabase = _redisConnection.GetDatabase();
            //        break;
            //    }
            //    catch
            //    {
            //        _logger.LogError(Error.NoConnectionToRedis);
            //        Task.Delay(10000).Wait();
            //    }
            //}

            //if (connectionStrings["ElasticClient"] == null)
            //{
            //    _logger?.LogError(Error.FailedToReadElasticConnectionString);
            //    Environment.Exit(1);
            //}
            //while (true)
            //{
            //    try
            //    {
            //        var settings = new ConnectionSettings(new Uri(connectionStrings["ElasticConnection"]!));
            //        _elasticClient = new ElasticClient(settings);
            //    }
            //    catch
            //    {
            //        _logger.LogError(Error.NoConnectionToElastic);
            //        Environment.Exit(1);
            //    }
            //}
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            GetLocalNetworkMachineNames();
        }

        private void GetLocalNetworkMachineNames()
        {
            try
            {
                var networkInterfaces = NetworkInterface.GetAllNetworkInterfaces();
                foreach (var networkInterface in networkInterfaces)
                {
                    if (networkInterface.OperationalStatus == OperationalStatus.Up 
                        && !networkInterface.Description.ToLowerInvariant().Contains("virtual"))
                    {
                        var ipProperties = networkInterface.GetIPProperties();
                        foreach (var ipAddress in ipProperties.UnicastAddresses) 
                        {
                            if (ipAddress.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork
                                && !IPAddress.IsLoopback(ipAddress.Address))
                            {
                                var networkAddress = ipAddress.Address.ToString();
                                var networkAddressParts = networkAddress.Split('.');
                                var subnet = $"{networkAddressParts[0]}.{networkAddressParts[1]}.{networkAddressParts[2]}";

                                using (var ping = new Ping())
                                {
                                    for (int i = 1; i < 255; i++)
                                    {
                                        var targetIP = $"{subnet}.{i}";
                                        var reply = ping.Send(targetIP, 100);
                                        if (reply != null && reply.Status == IPStatus.Success)
                                        {
                                            var hostEntry = Dns.GetHostEntry(targetIP);
                                            Console.WriteLine($"{targetIP} - {hostEntry.HostName}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
