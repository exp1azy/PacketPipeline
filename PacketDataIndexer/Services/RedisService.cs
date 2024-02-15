using Newtonsoft.Json;
using PacketDataIndexer.Resources;
using StackExchange.Redis;
using WebSpectre.Shared;

namespace PacketDataIndexer.Services
{
    /// <summary>
    /// Сервис, представляющий логику для взаимодействия с сервером Redis.
    /// </summary>
    internal class RedisService
    {
        private IDatabase _redisDatabase;
        private ConnectionMultiplexer? _redisConnection;
        private readonly ILogger<PacketPipeline> _logger;

        /// <summary>
        /// Конструктор.
        /// </summary>
        /// <param name="logger">Логи.</param>
        public RedisService(ILogger<PacketPipeline> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Подключение к серверу Redis.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        /// <returns></returns>
        public async Task ConnectAsync(string connectionString, int delay = 10)
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
                    await Task.Delay(TimeSpan.FromSeconds(delay));
                }
            }
        }

        /// <summary>
        /// Метод читает указанное количество элементов из потока Redis по указанному ключу и с указанным смещением.
        /// </summary>
        /// <param name="key">Ключ.</param>
        /// <param name="position">Позиция.</param>
        /// <param name="count">Количество.</param>
        /// <returns>Массив элементов.</returns>
        public async Task<StreamEntry[]> ReadStreamAsync(RedisKey key, RedisValue position, int count) =>
            await _redisDatabase.StreamReadAsync(key, position, count);

        /// <summary>
        /// Метод, возвращающий агентов из сервера Redis.
        /// </summary>
        /// <returns>Список ключей.</returns>
        public IEnumerable<RedisKey> GetRedisKeys(string host, int port)
        {
            IServer? server = default;

            try
            {
                server = _redisConnection!.GetServer(host, port);
            }
            catch
            {
                _logger.LogError(Error.NoConnectionToRedisServer);
                Environment.Exit(1);
            }

            foreach (var key in server!.Keys(pattern: "host_*"))
                yield return new RedisKey(key);
        }

        /// <summary>
        /// Метод очистки потоков Redis от устаревших данных.
        /// </summary>
        /// <param name="agents">Список агентов.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        public async Task ClearRedisStreamAsync(int timeout, int ttl, IEnumerable<RedisKey> agents, CancellationToken stoppingToken)
        {
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
    }
}
