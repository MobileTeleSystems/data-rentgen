# REST API сервер { #server }

REST API сервер Data.Rentgen предоставляет простой HTTP API для доступа к сущностям, хранящимся в [`базе данных`][database].
Реализован с использованием [FastAPI](https://fastapi.tiangolo.com/).

## Установка и запуск

### С Docker

- Установите [Docker](https://docs.docker.com/engine/install/)

- Установите [docker-compose](https://github.com/docker/compose/releases/)

- Выполните следующую команду:

  ```console
  $ docker compose --profile server up -d --wait
  ...
  ```

  `docker-compose` загрузит все необходимые образы, создаст контейнеры и запустит сервер.

  Параметры можно задать через файл `.env` или раздел `environment` в `docker-compose.yml`

??? note "docker-compose.yml"

    ```yaml hl_lines="71-99" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="22-27" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

- После запуска и готовности сервера откройте [http://localhost:8000/docs](http://localhost:8000/docs).

### Без Docker

- Установите Python 3.10 или выше

- Настройте [`базу данных`][database], выполните миграции и создайте партиции

- Создайте виртуальное окружение

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ```

- Установите пакет `data-rentgen` со следующими *дополнительными* зависимостями:

  ```console
  $ pip install data-rentgen[server,postgres]
  ...
  ```

- Запустите процесс сервера

  ```console
  $ python -m data_rentgen.server --host 0.0.0.0 --port 8000
  ...
  ```

  Это тонкая обёртка вокруг CLI [uvicorn](https://www.uvicorn.org/#command-line-options), параметры и команды точно такие же.

- После запуска и готовности сервера откройте [http://localhost:8000/docs](http://localhost:8000/docs).

## См. также

- [Аутентификация и авторизация][auth-server]
- [Конфигурация REST API сервера][configuration-server]
- [OpenAPI спецификация][server-openapi]
