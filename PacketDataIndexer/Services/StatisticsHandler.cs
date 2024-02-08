using PacketDataIndexer.Entities.ES;
using PacketDataIndexer.Entities;

namespace PacketDataIndexer.Services
{
    /// <summary>
    /// Предоставляет логику обработки статистики по входящему сетевому трафику.
    /// </summary>
    internal class StatisticsHandler
    {
        /// <summary>
        /// Формирование документа <see cref="Statistics"/> для ElasticSearch.
        /// </summary>
        /// <param name="statistics">Экземпляр статистики.</param>
        /// <param name="agent">Агент.</param>
        /// <returns>Документ.</returns>
        public StatisticsDocument GenerateStatisticsDocument(Statistics statistics, string agent) => new StatisticsDocument
        {
            Id = Guid.NewGuid(),
            Agent = agent,
            Statistics = statistics
        };
    }
}
