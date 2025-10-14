# Задание 10

## Общие положения

В рамках выполнения задания 10 реализовано масштабирование развёртывания сервиса предсказаний мошеннеческих транзакций в кластере kubernetes, разворачиваемого на отдельной виртуальной машине посредством автоматического запуска установочного скрипта при создании VM из terraform.

Сервис airflow из docker-compose также переместился в kubernetes, настроено автоматическо обновление DAG-ов из github-репозитория (этого), подпапки dags.

В кластере также развёрнуты сервисы для мониторинга Prometeus и Graphana, собирающие основные метрики сервисов и позволяющие возможность для нагрядного наблюдения. Для использования в Prometeus метрик реализованного приложения соответствущий код fast-api (src/otus_mlops/fast_api/application.py) был расширен добавлением счётчиков обращения к inference-у моделей, а также гистограммами для отслеживания производительности модели.

В Graphanа-e настроены 2 группы дашбордов, одна из которых ответственная за мониторинг развёртывания и отображает график изменения количества развёрнутых instance-ов сервиса с моделью в реальном времени, вторая -- показывает внутренние метрики fast-api приложения.

- [graphana_replicas_count](docs/hometasks/10/graphana-replicas-count.png)
- [graphana_model_performance_metrics](docs/hometasks/10/model_performance_metrics.png)

В Graphana настроена автоматическая отправка alert-сообщений в соответствующий tg-бот-канал для информирования заинтересованных лиц о ситуациях, когда в кластере одновременно развёрнуто максимальное количество instance-во (6 штук) и загрузка CPU хотя бы одного из них превышает 80%.

- [graphana_alert_rules](docs/hometasks/10/graphana-alert-rules.png)

Для симуляции такой нагрузки используется shell-скрипт, запускающийся в образе confluentinc/cp-kafkacat:latest. Он забирает данные из очереди kafka и направляет запрос на inference в развёрнутый fast-api сервис с моделью. В результате чего мы получам соответствующее оповещение.

- [alerting](docs/hometasks/10/alerting.png)
- [alert_message](docs/hometasks/10/alerts/message_alert.txt)

После того, как нагрузка на сервис снижается, мы получает повторное оповещение о стабилизации сервиса.

- [alerting_resolved](docs/hometasks/10/bot-alerting.png)
- [alert_message_resolved](docs/hometasks/10/alerts/message_resolved.txt)

В Graphana можно наблюдать за описанной ситуацией.

- [graphana_replicas_count](docs/hometasks/10/graphana-replicas-count.png)

Развёртывание kubernetes осуществляется при помощи Helm.
