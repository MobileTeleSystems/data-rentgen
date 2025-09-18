# Архитектура { #Architecture }

## Компоненты

Data.Rentgen построен с использованием следующих компонентов:

- Клиенты [OpenLineage](https://openlineage.io/docs/) и интеграции со сторонними модулями (например, Apache Spark, Apache Airflow).
- [`message-broker`][message-broker], получающий события в формате JSON.
- [`message-consumer`][message-consumer], обрабатывающий JSON-сообщения.
- [`database`][database] для хранения обработанных и очищенных данных.
- [`server`][server], предоставляющий данные из базы данных.
- [`frontend`][frontend], обращающийся к REST API для навигации по созданным сущностям и графу линейности.
- [`http2kafka`][http2kafka] (опционально), прокси для отправки событий OpenLineage в Kafka через HTTP API.

## Диаграмма архитектуры

```plantuml

    @startuml
        title Архитектура Data.Rentgen
        skinparam componentStyle rectangle
        left to right direction

        frame "Data.Rentgen" {
            queue "Kafka" as KAFKA
            component "Потребитель сообщений" as CONSUMER
            database "PostgreSQL" as DB
            component "REST API сервер" as API
            component "Фронтенд" as FRONTEND
            component "HTTP2Kafka" as HTTP2KAFKA
        }

        frame "OpenLineage" {
            agent "OpenLineage Spark" as SPARK
            agent "OpenLineage Airflow" as AIRFLOW
            agent "OpenLineage Hive" as HIVE
            agent "OpenLineage Flink" as FLINK
            agent "OpenLineage dbt" as DBT
            agent "OpenLineage прочие" as OTHER
            agent "OpenLineage KafkaTransport" as KAFKA_TRANSPORT
            agent "OpenLineage HttpTransport" as HTTP_TRANSPORT
        }

        actor "Пользователь" as USER

        [SPARK] --> [KAFKA_TRANSPORT]
        [AIRFLOW] --> [KAFKA_TRANSPORT]
        [HIVE] --> [KAFKA_TRANSPORT]
        [FLINK] --> [KAFKA_TRANSPORT]
        [DBT] --> [KAFKA_TRANSPORT]
        [KAFKA_TRANSPORT] --> [KAFKA]

        [OTHER] --> [HTTP_TRANSPORT]
        [HTTP_TRANSPORT] --> [HTTP2KAFKA]
        [HTTP2KAFKA] --> [KAFKA]

        [KAFKA] --> [CONSUMER]
        [CONSUMER] --> [DB]

        [API] --> [DB]
        [FRONTEND] --> [API]
        [USER] --> [FRONTEND]

    @enduml
```
