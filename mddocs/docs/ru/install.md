# Установка Data.Rentgen { #overview-install }

## Требования

- [Docker](https://docs.docker.com/engine/install/)
- [docker-compose](https://github.com/docker/compose/releases/)

## Установка и запуск

Скопируйте `docker-compose.yml` и `.env.docker` из этого репозитория:

??? note "docker-compose.yml"

    ```yaml hl_lines="101-118 177" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="7-20" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

Затем запустите контейнеры с помощью `docker-compose`:

    ```console
    $ VERSION=latest docker compose --profile all up -d --wait
    ...
    ```

`docker-compose` загрузит необходимые образы, создаст контейнеры и запустит их в правильном порядке. Параметры можно задать через файл `.env.docker` или секцию `environment` в `docker-compose.yml`.

`VERSION` — это тег docker-образа. Все доступные теги можно найти [здесь](https://hub.docker.com/r/mtsrus/data-rentgen/tags).

### Доступ к Data.Rentgen

После того как все контейнеры запущены и готовы к работе, вы можете:

- Открыть веб-интерфейс по адресу <http://localhost:3000>
- Просмотреть документацию REST API Swagger по адресу <http://localhost:8000/docs>
