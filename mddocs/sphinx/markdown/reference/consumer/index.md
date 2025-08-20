<a id="message-consumer"></a>

# Message Consumer

Data.Rentgen fetches messages from a [Message Broker](../broker/index.md#message-broker) using a [FastStream](https://faststream.airt.ai) based consumer, parses incoming messages,
and creates all parsed entities in the [Relation Database](../database/index.md#database). Malformed messages are send back to broker, to different topic.

## Install & run

### With docker

* Install [Docker](https://docs.docker.com/engine/install/)
* Install [docker-compose](https://github.com/docker/compose/releases/)
* Run the following command:
  ```console
  $ docker compose --profile consumer up -d --wait
  ```

  `docker-compose` will download all necessary images, create containers, and then start consumer process.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

  ### `docker-compose.yml`

  ```default
  services:
    db:
      image: postgres:17
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 5432:5432
      volumes:
        - postgres_data:/var/lib/postgresql/data
      healthcheck:
        test: pg_isready
        start_period: 5s
        interval: 5s
        timeout: 5s
        retries: 3

    db-migration:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: |
        python -m data_rentgen.db.migrations upgrade head
      env_file: .env.docker
      depends_on:
        db:
          condition: service_healthy

    db-create-partitions:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: |
        python -m data_rentgen.db.scripts.create_partitions
      env_file: .env.docker
      depends_on:
        db-migration:
          condition: service_completed_successfully

    db-refresh-views:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: |
        python -m data_rentgen.db.scripts.refresh_analytic_views
      env_file: .env.docker
      depends_on:
        db-migration:
          condition: service_completed_successfully
      profiles:
        - analytics
        - all

    db-cleanup-partitions:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: |
        python -m data_rentgen.db.scripts.cleanup_partitions truncate --keep-after $(date --date='-1year' '+%Y-%m-%d')
      env_file: .env.docker
      depends_on:
        db-migration:
          condition: service_completed_successfully
      profiles:
        - cleanup
        - all

    db-seed:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: |
        python -m data_rentgen.db.scripts.seed
      env_file: .env.docker
      depends_on:
        db-migration:
          condition: service_completed_successfully
      profiles:
        - seed
        - all

    server:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: python -m data_rentgen.server --host 0.0.0.0 --port 8000
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 8000:8000
      # PROMETHEUS_MULTIPROC_DIR is required for multiple workers, see:
      # https://prometheus.github.io/client_python/multiprocess/
      environment:
        PROMETHEUS_MULTIPROC_DIR: /tmp/prometheus-metrics
      # tmpfs dir is cleaned up each container restart
      tmpfs:
        - /tmp/prometheus-metrics:mode=1777
      healthcheck:
        test: [CMD-SHELL, curl -f http://localhost:8000/monitoring/ping]
        interval: 30s
        timeout: 5s
        retries: 3
        start_period: 5s
      depends_on:
        db:
          condition: service_healthy
        db-migration:
          condition: service_completed_successfully
      profiles:
        - server
        - frontend
        - all

    broker:
      image: bitnami/kafka:3.9
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 9093:9093
      volumes:
        - kafka_data:/bitnami/kafka
      healthcheck:
        test: [CMD-SHELL, kafka-topics.sh --bootstrap-server 127.0.0.1:9095 --list]
        interval: 10s
        timeout: 5s
        retries: 5
      profiles:
        - broker
        - consumer
        - http2kafka
        - all

    consumer:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: python -m data_rentgen.consumer --host 0.0.0.0 --port 8000
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 8001:8000
      healthcheck:
        test: [CMD-SHELL, curl -f http://localhost:8000/monitoring/ping]
        interval: 30s
        timeout: 5s
        retries: 3
        start_period: 5s
      depends_on:
        broker:
          condition: service_healthy
        db-migration:
          condition: service_completed_successfully
      profiles:
        - consumer
        - all

    frontend:
      image: mtsrus/data-rentgen-ui:${VERSION:-latest}
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 3000:3000
      depends_on:
        server:
          condition: service_healthy
      profiles:
        - frontend
        - all

    http2kafka:
      image: mtsrus/data-rentgen:${VERSION:-latest}
      command: python -m data_rentgen.http2kafka --host 0.0.0.0 --port 8000
      restart: unless-stopped
      env_file: .env.docker
      ports:
        - 8002:8000
      healthcheck:
        test: [CMD-SHELL, curl -f http://localhost:8000/monitoring/ping]
        interval: 30s
        timeout: 5s
        retries: 3
        start_period: 5s
      depends_on:
        broker:
          condition: service_healthy
      profiles:
        - http2kafka
        - all

  volumes:
    postgres_data:
    kafka_data:
  ```

  ### `.env.docker`

  ```default
  # Init Postgres database
  POSTGRES_DB=data_rentgen
  POSTGRES_USER=data_rentgen
  POSTGRES_PASSWORD=changeme
  POSTGRES_INITDB_ARGS=--encoding=UTF-8 --lc-collate=C --lc-ctype=C

  # Init Kafka
  KAFKA_CFG_NODE_ID=0
  KAFKA_CFG_PROCESS_ROLES=controller,broker
  KAFKA_CFG_LISTENERS=DOCKER://:9092,LOCALHOST://:9093,CONTROLLER://:9094,INTERBROKER://:9095
  KAFKA_CFG_ADVERTISED_LISTENERS=DOCKER://broker:9092,LOCALHOST://localhost:9093,INTERBROKER://broker:9095
  KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERBROKER:PLAINTEXT,DOCKER:SASL_PLAINTEXT,LOCALHOST:SASL_PLAINTEXT
  KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@broker:9094
  KAFKA_CFG_INTER_BROKER_LISTENER_NAME=DOCKER
  KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
  KAFKA_CFG_SASL_MECHANISM_CONTROLLER_PROTOCOL=PLAIN
  KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN
  KAFKA_CLIENT_USERS=data_rentgen
  KAFKA_CLIENT_PASSWORDS=changeme
  KAFKA_CFG_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256

  # Common backend config
  DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://data_rentgen:changeme@db:5432/data_rentgen
  DATA_RENTGEN__LOGGING__PRESET=colored

  # See Backend -> Server -> Configuration documentation
  DATA_RENTGEN__SERVER__DEBUG=false

  # See Backend -> Consumer -> Configuration documentation
  DATA_RENTGEN__KAFKA__BOOTSTRAP_SERVERS=["broker:9092"]
  DATA_RENTGEN__KAFKA__SECURITY__TYPE=SCRAM-SHA-256
  DATA_RENTGEN__KAFKA__SECURITY__USER=data_rentgen
  DATA_RENTGEN__KAFKA__SECURITY__PASSWORD=changeme
  DATA_RENTGEN__KAFKA__COMPRESSION=zstd

  # See Frontend -> UI
  DATA_RENTGEN__UI__API_BROWSER_URL=http://localhost:8000

  # Session
  DATA_RENTGEN__SERVER__SESSION__SECRET_KEY=session_secret_key

  # Keycloak Auth
  DATA_RENTGEN__AUTH__KEYCLOAK__SERVER_URL=http://keycloak:8080
  DATA_RENTGEN__AUTH__KEYCLOAK__REALM_NAME=create_realm_manually
  DATA_RENTGEN__AUTH__KEYCLOAK__CLIENT_ID=create_client_manually
  DATA_RENTGEN__AUTH__KEYCLOAK__CLIENT_SECRET=generated_by_keycloak
  DATA_RENTGEN__AUTH__KEYCLOAK__REDIRECT_URI=http://localhost:3000/auth-callback
  DATA_RENTGEN__AUTH__KEYCLOAK__SCOPE=email
  DATA_RENTGEN__AUTH__KEYCLOAK__VERIFY_SSL=False
  DATA_RENTGEN__AUTH__PROVIDER=data_rentgen.server.providers.auth.keycloak_provider.KeycloakAuthProvider

  # Dummy Auth
  DATA_RENTGEN__AUTH__PROVIDER=data_rentgen.server.providers.auth.dummy_provider.DummyAuthProvider
  DATA_RENTGEN__AUTH__ACCESS_TOKEN__SECRET_KEY=access_key_secret

  # Personal Tokens
  export DATA_RENTGEN__AUTH__PERSONAL_TOKENS__ENABLED=True
  export DATA_RENTGEN__AUTH__PERSONAL_TOKENS__SECRET_KEY=pat_secret

  # Cors
  DATA_RENTGEN__SERVER__CORS__ENABLED=True
  DATA_RENTGEN__SERVER__CORS__ALLOW_ORIGINS=["http://localhost:3000"]
  DATA_RENTGEN__SERVER__CORS__ALLOW_CREDENTIALS=True
  DATA_RENTGEN__SERVER__CORS__ALLOW_METHODS=["*"]
  DATA_RENTGEN__SERVER__CORS__ALLOW_HEADERS=["*"]
  DATA_RENTGEN__SERVER__CORS__EXPOSE_HEADERS=["X-Request-ID","Location","Access-Control-Allow-Credentials"]
  ```

### Without docker

* Install Python 3.10 or above
* Setup [Relation Database](../database/index.md#database), run migrations and create partitions
* Setup [Message Broker](../broker/index.md#message-broker)
* Create virtual environment
  ```console
  $ python -m venv /some/.venv
  $ source /some/.venv/activate
  ```
* Install `data-rentgen` package with following *extra* dependencies:
  ```console
  $ pip install data-rentgen[consumer,postgres]
  ```

  #### NOTE
  For `SASL_GSSAPI` auth mechanism you also need to install system packages providing `kinit` and `kdestroy` binaries:
  ```console
  $ apt install libkrb5-dev krb5-user gcc make autoconf  # Debian-based
  $ dnf install krb5-devel krb5-libs krb5-workstation gcc make autoconf  # CentOS, OracleLinux
  ```

  And then install `gssapi` extra:
  ```console
  $ pip install data-rentgen[consumer,postgres,gssapi]
  ```
* Run consumer process
  ```console
  $ python -m data_rentgen.consumer
  ```

## See also

* [Consumer configuration](configuration/index.md)
