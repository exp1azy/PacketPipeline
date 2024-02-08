using PacketDataIndexer.Entities.ES;
using PacketDotNet;

namespace PacketDataIndexer.Interfaces
{
    /// <summary>
    /// Предоставляет методы для обработки входящих пакетов.
    /// </summary>
    internal interface INetwork
    {
        /// <summary>
        /// Метод извлечения пакета.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет.</returns>
        public object? Extract(Packet packet);

        /// <summary>
        /// Формирование документа <see cref="BasePacketDocument"/> для ElasticSearch.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <param name="transportId">Идентификатор для пакета транспортного уровня.</param>
        /// <param name="networkId">Идентификатор для пакета сетевого уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <returns>Сформированный документ для ElasticSearch, содержащий пакет.</returns>
        public BasePacketDocument? GenerateDocument(object packet, Guid? transportId, Guid? networkId, string agent);
    }
}
