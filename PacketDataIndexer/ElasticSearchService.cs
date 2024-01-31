using Elasticsearch.Net;
using Nest;
using PacketDataIndexer.Resources;
using Error = PacketDataIndexer.Resources.Error;

namespace PacketDataIndexer
{
    internal class ElasticSearchService
    {
        private ElasticClient _elasticClient;
        private readonly IConfiguration _config;
        private readonly ILogger<PacketPipeline> _logger;

        public ElasticSearchService(IConfiguration config, ILogger<PacketPipeline> logger)
        {
            _config = config;
            _logger = logger;

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

            ConnectAsync(elasticConnection!, authParams["Username"]!, authParams["Password"]!).Wait();
        }

        /// <summary>
        /// Подключение к серверу ElasticSearch.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        /// <param name="username">Имя пользователя.</param>
        /// <param name="password">Пароль.</param>
        /// <returns></returns>
        public async Task ConnectAsync(string connectionString, string username, string password)
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
        /// Массовая загрузка данных в индекс ElasticSearch.
        /// </summary>
        /// <param name="bulkDescriptor">Дескриптор.</param>
        /// <param name="stoppingToken">Токен остановки.</param>
        /// <returns></returns>
        public async Task BulkAsync(BulkDescriptor bulkDescriptor, CancellationToken stoppingToken)
        {
            try
            {
                await _elasticClient.BulkAsync(bulkDescriptor, stoppingToken);
            }
            catch (NullReferenceException) { }
            catch (Exception ex)
            {
                _logger.LogWarning(Warning.AnErrorOccuredWhileIndexing, ex.Message);
            }
        }
    }
}
