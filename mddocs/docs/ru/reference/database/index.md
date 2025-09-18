# База данных { #database }

Data.Rentgen использует реляционную базу данных для хранения сущностей происхождения данных и связей между ними.

В настоящее время Data.Rentgen поддерживает только [PostgreSQL](https://www.postgresql.org/), поскольку полагается на партиционирование таблиц, полнотекстовый поиск и специфические функции агрегации.

## Миграции

После запуска базы данных необходимо выполнить скрипт миграции. Если база данных пуста, он создаст все необходимые таблицы и индексы. Если база данных не пуста, будет выполнено обновление структуры базы данных.

Скрипт миграции — это тонкая обертка над [Alembic cli](https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration), параметры и команды точно такие же.

!!! warning "Внимание"

  Другие контейнеры (consumer, server) должны быть остановлены во время выполнения миграций, чтобы предотвратить вмешательство.

## Партиции

После выполнения миграций необходимо запустить [`create-partitions-cli`][create-partitions-cli], который создает партиции для некоторых таблиц в базе данных.
По умолчанию он создает ежемесячные партиции для текущего и следующего месяца. Это можно изменить, переопределив аргументы команды.

Этот скрипт должен выполняться по расписанию, в зависимости от гранулярности партиций.
Планирование можно выполнить, добавив специальную запись в [crontab](https://help.ubuntu.com/community/CronHowto).

Настоятельно рекомендуется также добавить в cron скрипт очистки старых партиций [`cleanup-partitions-cli`][cleanup-partitions-cli].
Настройка планирования такая же, как и для создания партиций.

## Аналитические представления

Вместе с миграциями создается несколько аналитических представлений. Они управляются через [`refresh-analytic-views-cli`][refresh-analytic-views-cli] и должны выполняться по расписанию.

## Заполнение данными

По умолчанию база данных создается без данных. Для заполнения базы данных примерами используйте [`db-seed-cli`][db-seed-cli].

## Требования

- PostgreSQL 12 или выше. Рекомендуется использовать последнюю версию Postgres.

## Установка и запуск

### С Docker

- Установите [Docker](https://docs.docker.com/engine/install/)

- Установите [docker-compose](https://github.com/docker/compose/releases/)

- Выполните следующую команду:

  ```console
  $ docker compose --profile analytics,cleanup,seed up -d
  ...
  ```

  `docker-compose` загрузит образ PostgreSQL, создаст контейнер и том, а затем запустит контейнер.
  Точка входа образа создаст базу данных, если том пуст.

  После этого запустится несколько одноразовых контейнеров:

  - `db-create-partitions` создаст необходимые партиции в БД.
  - `db-cleanup-partitions` очистит старые партиции.
  - `db-refresh-views` обновит аналитические представления.
  - `db-seed` заполнит базу данных примерами (необязательно, можно пропустить).

  Параметры можно установить через файл `.env` или секцию `environment` в `docker-compose.yml`

??? note "docker-compose.yml"

    ```yaml hl_lines="1-69 176" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="1-5 23" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

- Добавьте скрипты в crontab:

  ```console
  $ crontab -e
  ...
  ```

  ```text
  0 0 * * * docker compose -f "/path/to/docker-compose.yml" start db-create-partitions db-refresh-views db-cleanup-partitions
  ```

### Без Docker

- Для установки PostgreSQL следуйте [инструкции по установке](https://www.postgresql.org/download/).

- Установите Python 3.10 или выше

- Создайте виртуальное окружение

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ```

- Установите пакет `data-rentgen` со следующими *дополнительными* зависимостями:

  ```console
  $ pip install data-rentgen[postgres]
  ...
  ```

- Настройте [`подключение к базе данных`][configuration-database] используя переменные окружения, например, создав файл `.env`:

  ```console title="/some/.env"

  $ export DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://data_rentgen:changeme@localhost:5432/data_rentgen
  ...
  ```

  А затем прочитайте значения из этого файла:

  ```console
  $ source /some/.env
  ...
  ```

- Выполните миграции:

  ```console
  $ python -m data_rentgen.db.migrations upgrade head
  ...
  ```

!!! note "Примечание"

  Эта команда должна выполняться после каждого обновления до новой версии Data.Rentgen.

- Создайте партиции:

  ```console
  $ python -m data_rentgen.db.scripts.create_partitions
  ...
  ```

- Создайте аналитические представления:

  ```console
  $ python -m data_rentgen.db.scripts.refresh_analytic_views
  ...
  ```

- Заполните базу данных примерами данных (необязательно, можно пропустить):

  ```console
  $ python -m data_rentgen.db.scripts.seed
  ...
  ```

- Добавьте скрипты в crontab:

  ```console
  $ crontab -e
  ...
  ```

  ```text
  # читаем настройки из файла .env и запускаем скрипт используя определенное venv со всеми необходимыми зависимостями
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.create_partitions"
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.cleanup_partitions truncate --keep-after $(date --date='-1year' '+%Y-%m-%d')"
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.refresh_analytic_views"
  ```

## См. также

[Конфигурация][configuration]
[CLI создания партиций][create-partitions-cli]
[CLI очистки партиций][cleanup-partitions-cli]
[CLI обновления аналитических представлений][refresh-analytic-views-cli]
[CLI заполнения данными][db-seed-cli]
[Структура][database-structure]
