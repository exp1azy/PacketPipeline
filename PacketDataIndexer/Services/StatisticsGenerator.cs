using WebSpectre.Shared;
using WebSpectre.Shared.ES;

namespace PacketDataIndexer.Services
{
    /// <summary>
    /// Предоставляет логику обработки статистики по входящему сетевому трафику.
    /// </summary>
    public static class StatisticsGenerator
    {
        /// <summary>
        /// Формирование документа <see cref="Statistics"/> для ElasticSearch.
        /// </summary>
        /// <param name="statistics">Экземпляр статистики.</param>
        /// <param name="agent">Агент.</param>
        /// <returns>Документ.</returns>
        public static StatisticsDocument GenerateStatisticsDocument(Statistics statistics, string agent) => new StatisticsDocument
        {
            Id = Guid.NewGuid(),
            Agent = agent,
            Statistics = statistics
        };
    }
}
