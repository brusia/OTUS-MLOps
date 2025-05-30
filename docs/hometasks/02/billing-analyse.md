# Затраты на поддержание работоспособности spark-кластера (в месяца)

proxy 1603.15
masternode 4449.17
datanodes 7899.72 x 3 = 23699.16
dataproc cluster = 28389.6

## Сравнение cодержания spark-кластера и объектного хранилища

object_storage менее 200 рублей в месяц. Различие в сотню раз!

## Оптимизация затрат

В качестве оптимизации затрат на содержание инфраструктуры предлагается разделить инфраструктуру на два модуля: объектное хранилище, которое требует меньше затрат на содержание и которое будет доступно всегда (даже при простоях) и dataroс-кластер соответствующей инфраструктурой (сетью, виртуальной машиной и remote-exec). Управление же инфраструктурой будет осуществляться с локальной машины посредством скрипта compute (реализовано в Makefile).
Последовательность выполняемых действий:

1. Создание dataproc-кластера и копирование на него данных из целевого бакета (при запуске скриптов также проверяется, что целевой бакет не пуст. Если он не содержит файлов, то происходит копирования из предоставленного бакета otus)
2. Выполнение полезной работы: запуск целевых функций, работа с данными, подготовка, обучение и т.д.
3. Сохранение необходимых артефактов выполнения полезной работы в целевом бакете облачного хранилища (не удаляемого) -- будет добавлено при выполнении задания 3 -- частично реализовано. В текущей реализации происходит двойное копирование: сперва реультатов выполнения "полезных скриптов" -- файла dataproc_master_execution.log на прокси-машину, и уже оттуда сохранение на object storage, что добавляет дополнительные расходы на передачу файлов по сети и увеличивает общий overhead
4. Удаление dataproc-кластера по завершению работы или по завершению одного из предыдущих шагов с ошибкой
5. Контроль корректного удаления (не реализовано)

Это поможет минимизировать затраты на использование dataproc-кластера (т.к. мы будет платить только за время его фактического использования)

### Среди ограничений предложенного подхода можно выделить

- Возрастающие затраты на содержание объектного хранилища (в данном случае издержки незначительны)
- Увеличение времени ожидания (для каждой новой задачи потребуется всякий раз создавать заново и затем удалять соответствующую инфраструктуру). Это также значительно увеличит время отладки разрабатываемых программых продуктов.
- Отсутствие информирования об ошибках, а также неконтролируемость корректности выполнения всех шагов (в случае возникновения непредвиденных ошибок, например, при выполнении полезного кода или сохранении артефактов работы есть риск оставить развёрнутую инфраструктуры, пожирающую бюджет)
Это решается аккуратным написанием скриптов make compute и внимательной их отладкой, а также настройкой алёртинга.
