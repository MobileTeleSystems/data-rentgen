# HTTP2Kafka прокси { #http2kafka }

Некоторые интеграции OpenLineage поддерживают только HttpTransport, но не KafkaTransport, например Trino.

HTTP → Kafka прокси Data.Rentgen — это опциональный компонент, который предоставляет простой HTTP API для получения
[событий выполнения OpenLineage](https://openlineage.io/docs/spec/object-model) в формате JSON и отправки их в топик Kafka как есть, чтобы они могли быть обработаны {ref}`message-consumer` соответствующим образом.

## OpenLineage HttpTransport или KafkaTransport?

Введение http2kafka в цепочку немного снижает производительность:

- Он парсит все входящие события для целей валидации и маршрутизации. Чем больше событие, тем медленнее парсинг.
- Протокол HTTP/HTTPS гораздо сложнее TCP протокола Kafka и изначально имеет гораздо большую задержку.

Если интеграция OpenLineage поддерживает и HttpTransport, и KafkaTransport, а Kafka не использует сложную аутентификацию, не поддерживаемую OpenLineage (например, OAUTHBEARER), предпочтительно выбирать KafkaTransport.

Если это невозможно, http2kafka — это правильный выбор.

## Установка и запуск

### С помощью docker

- Установите [Docker](https://docs.docker.com/engine/install/)

- Установите [docker-compose](https://github.com/docker/compose/releases/)

- Выполните следующую команду:

  ```console
  $ docker compose --profile http2kafka up -d --wait
  ...
  ```

  `docker-compose` загрузит все необходимые образы, создаст контейнеры и запустит компонент.

  Опции можно задать через файл `.env` или секцию `environment` в `docker-compose.yml`

<!-- TODO везде, где literal include нужно сделать инклюды-->>

??? note "docker-compose.yml"

    ```yaml hl_lines="155-173" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="29-34" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

- После запуска и готовности компонента откройте <http://localhost:8002/docs>.

### Без docker

- Установите Python 3.10 или выше

- Настройте {ref}`message-broker`

- Создайте виртуальное окружение

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ```

- Установите пакет `data-rentgen` со следующими *дополнительными* зависимостями:

  ```console
  $ pip install data-rentgen[http2kafka]
  ...
  ```

- Запустите процесс http2kafka

  ```console
  $ python -m data_rentgen.http2kafka --host 0.0.0.0 --port 8002
  ...
  ```

  Это тонкая обертка вокруг [uvicorn](https://www.uvicorn.org/#command-line-options) cli,
  опции и команды точно такие же.

- После запуска и готовности сервера откройте [http://localhost:8002/docs](http://localhost:8002/docs).

## См. также

[Конфигурация][configuration-http2kafka]
[OpenAPI][http2kafka-openapi]
[Альтернативы][http2kafka-alternatives]
