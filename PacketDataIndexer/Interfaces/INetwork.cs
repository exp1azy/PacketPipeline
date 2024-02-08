using PacketDataIndexer.Entities.ES;
using PacketDotNet;

namespace PacketDataIndexer.Interfaces
{
    internal interface INetwork
    {
        /// <summary>
        /// Метод, извлекающий пакет.
        /// </summary>
        /// <param name="packet">Пакет.</param>
        /// <returns>Извлеченный пакет сетевого уровня.</returns>
        public object? Extract(Packet packet);

        /// <summary>
        /// Формирование документа <see cref="BasePacketDocument"/> для ElasticSearch.
        /// </summary>
        /// <param name="packet">Неизвлеченный пакет.</param>
        /// <param name="transportId">Идентификатор для пакета транспортного уровня.</param>
        /// <param name="networkId">Идентификатор для пакета сетевого уровня.</param>
        /// <param name="agent">Агент.</param>
        /// <returns>Извлеченный пакет.</returns>
        public BasePacketDocument? GenerateDocument(object packet, Guid? transportId, Guid? networkId, string agent);
    }
}
