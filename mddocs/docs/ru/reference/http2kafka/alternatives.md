# Альтернативы { #http2kafka-alternatives }

HTTP → Kafka прокси реализован с обязательной аутентификацией, использующей персональные токены. Это может быть недостатком в некоторых ситуациях.

Если ваш случай использования требует отправки событий OpenLineage в DataRentgen через HTTP, но без какой-либо аутентификации, ниже описаны некоторые альтернативы.

## Fluentbit

[Fluentbit](https://fluentbit.io/) — это легковесный, но мощный обработчик логов, написанный на C.
Он может принимать [HTTP запросы](https://docs.fluentbit.io/manual/data-pipeline/inputs/http) и записывать тело в [топик Kafka](https://docs.fluentbit.io/manual/data-pipeline/outputs/kafka).

Пример конфигурации:

```yaml title="fluent-bit.yml"

pipeline:
    # принимаем HTTP запросы на порт 8002
    inputs:
      - name: http
        port: 8002
        mem_buf_limit: 50MB

    # Маршрутизация событий в партицию с использованием ключа сообщения
    filters:
      - name: lua
        match: '*'
        call: set_message_key
        code:  |
            function set_message_key(tag, timestamp, record)
            local new_record = record
            if record.run.facets.parent then
                if record.run.facets.parent.root then
                new_record.messageKey = "run:" .. record.run.facets.parent.root.job.namespace .. "/" .. record.run.facets.parent.root.job.name
                else
                new_record.messageKey = "run:" .. record.run.facets.parent.job.namespace .. "/" .. record.run.facets.parent.job.name
                end
            else
                new_record.messageKey = "run:" .. record.job.namespace .. "/" .. record.job.name
            end
            return 1, timestamp, new_record
            end

    # Записываем данные в топик Kafka
    outputs:
      - name: kafka
        match: '*'
        format: json
        timestamp_key: eventTime
        timestamp_format: iso8601_ns
        message_key_field: messageKey
        brokers: localhost:9093
        topics: input.runs
        rdkafka.security.protocol: SASL_PLAINTEXT
        rdkafka.sasl.mechanism: SCRAM-SHA-256
        rdkafka.sasl.username: data_rentgen
        rdkafka.sasl.password: changeme
        rdkafka.client.id: fluent-bit
        rdkafka.request.required.acks: 1
        rdkafka.log.connection.close: false
```

```yaml title="docker-compose.yml"

services:
    fluent-bit:
        image: central-mirror.services.mts.ru/fluent/fluent-bit
        restart: unless-stopped
        command: --config /fluent-bit/etc/fluent-bit.yml
        volumes:
        - ./fluent-bit.yml:/fluent-bit/etc/fluent-bit.yml
        # Имена хостов Kafka должны быть разрешимы из сети контейнера
        network_mode: host
```
