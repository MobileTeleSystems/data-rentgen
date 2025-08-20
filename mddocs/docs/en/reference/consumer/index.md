(message-consumer)=

# Message Consumer

Data.Rentgen fetches messages from a {ref}`message-broker` using a [FastStream](https://faststream.airt.ai) based consumer, parses incoming messages,
and creates all parsed entities in the {ref}`database`. Malformed messages are send back to broker, to different topic.

## Install & run

### With docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile consumer up -d --wait
  ```

  `docker-compose` will download all necessary images, create containers, and then start consumer process.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

  ```{eval-rst}
  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 120-138
  ```

  ```{eval-rst}
  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 22-24,29-34
  ```

### Without docker

- Install Python 3.10 or above

- Setup {ref}`database`, run migrations and create partitions

- Setup {ref}`message-broker`

- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  $ source /some/.venv/activate
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[consumer,postgres]
  ```

  :::{note}
  For `SASL_GSSAPI` auth mechanism you also need to install system packages providing `kinit` and `kdestroy` binaries:

  ```console
  $ apt install libkrb5-dev krb5-user gcc make autoconf  # Debian-based
  $ dnf install krb5-devel krb5-libs krb5-workstation gcc make autoconf  # CentOS, OracleLinux
  ```

  And then install `gssapi` extra:

  ```console
  $ pip install data-rentgen[consumer,postgres,gssapi]
  ```
  :::

- Run consumer process

  ```console
  $ python -m data_rentgen.consumer
  ```

## See also

```{toctree}
:maxdepth: 1

configuration/index
```
