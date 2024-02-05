using Elasticsearch.Net;
using Nest;
using PacketDataIndexer.Resources;
using Error = PacketDataIndexer.Resources.Error;

namespace PacketDataIndexer
{
    /// <summary>
    /// Сервис, представляющий логику для взаимодействия с сервером ElasticSearch.
    /// </summary>
    internal class ElasticSearchService
    {
        private ElasticClient _elasticClient;
        private readonly ILogger<PacketPipeline> _logger;

        /// <summary>
        /// Конструктор.
        /// </summary>
        /// <param name="logger">Логи.</param>
        public ElasticSearchService(ILogger<PacketPipeline> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Подключение к серверу ElasticSearch.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        /// <param name="username">Имя пользователя.</param>
        /// <param name="password">Пароль.</param>
        /// <returns></returns>
        public async Task ConnectAsync(string connectionString, string username, string password, int delay = 10)
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
                    await Task.Delay(TimeSpan.FromSeconds(delay));
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
