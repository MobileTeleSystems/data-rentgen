# Frontend { #frontend }

Data.Rentgen предоставляет [Frontend (UI)](https://github.com/MobileTeleSystems/data-rentgen-ui) на основе [ReactAdmin](https://marmelab.com/react-admin/) и [ReactFlow](https://reactflow.dev/), предоставляя пользователям возможность навигации по сущностям и построения графа происхождения данных (lineage).

## Установка и запуск

### С Docker

- Установите [Docker](https://docs.docker.com/engine/install/)

- Установите [docker-compose](https://github.com/docker/compose/releases/)

- Выполните следующую команду:

  ```console
  $ docker compose --profile frontend up -d --wait
  ...
  ```

  `docker-compose` загрузит образ Data.Rentgen UI, создаст контейнеры и запустит их.

  Опции можно задать через файл `.env` или секцию `environment` в `docker-compose.yml`

??? note "docker-compose.yml"

  ```yaml hl_lines="140-151" linenums="1"
  ----8<----
  docker-compose.yml
  ----8<----
  ```  

??? note ".env.docker"

  ```ini hl_lines="36-37" linenums="1"
  ----8<----
  .env.docker
  ----8<----
  ```

- После запуска и готовности frontend, откройте <http://localhost:3000>.

## См. также

[Конфигурация][configuration-frontend]
