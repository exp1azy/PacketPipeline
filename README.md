# PacketPipeline
Модуль системы мониторинга и анализа производительности сети, представляющий собой конвейер сетевого трафика, оформленный в виде службы Windows
#
- Из потоков распределенного кеша Redis получаем данные о сетевом взаимодействии агентов
- Десериализуем и обрабатываем данные
- Индексируем данные о сети для построения отчетов в ElasticSearch
