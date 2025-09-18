# Консьюмер сообщений { #message-consumer }

Data.Rentgen получает сообщения из [`брокера сообщений`][message-broker] с использованием консьюмера на основе [FastStream](https://faststream.airt.ai), парсит входящие сообщения и создает все распознанные сущности в [`базе данных`][database]. Некорректные сообщения отправляются обратно в брокер в другой топик.

## Установка и запуск

### С docker

- Установите [Docker](https://docs.docker.com/engine/install/)

- Установите [docker-compose](https://github.com/docker/compose/releases/)

- Выполните следующую команду:

  ```console
  $ docker compose --profile consumer up -d --wait
  ...
  ```

  `docker-compose` загрузит все необходимые образы, создаст контейнеры, а затем запустит процесс консьюмера.

  Параметры можно задать через файл `.env` или секцию `environment` в `docker-compose.yml`

??? note "docker-compose.yml"

  ```yaml hl_lines="120-140" linenums="1"
  ----8<----
  docker-compose.yml
  ----8<----
  ```

??? note ".env.docker"

  ```ini hl_lines="22-24 29-34" linenums="1"
  ----8<----
  .env.docker
  ----8<----
  ```

### Без docker

- Установите Python 3.10 или выше

- Настройте [`базу данных`][database], выполните миграции и создайте партиции

- Настройте [`брокер сообщений`][message-broker]

- Создайте виртуальное окружение

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ...
  ```

- Установите пакет `data-rentgen` со следующими *дополнительными* зависимостями:

  ```console
  $ pip install data-rentgen[consumer,postgres]
  ...
  ```

!!! note

    Для механизма аутентификации `SASL_GSSAPI` вам также необходимо установить системные пакеты, предоставляющие бинарные файлы `kinit` и `kdestroy`:

    ```console
    $ apt install libkrb5-dev krb5-user gcc make autoconf  # Основанные на Debian
    ...
    $ dnf install krb5-devel krb5-libs krb5-workstation gcc make autoconf  # CentOS, OracleLinux
    ...
    ```

    А затем установить дополнение `gssapi`:

    ```console
    $ pip install data-rentgen[consumer,postgres,gssapi]
    ...
    ```

- Запустите процесс консьюмера

  ```console
  $ python -m data_rentgen.consumer
  ...
  ```

## См. также

[Конфигурация консьюмера][configuration-consumer]
