(database)=

# Relation Database

Data.Rentgen uses relational database as a storage for lineage entities and relations.

Currently, Data.Rentgen supports only [PostgreSQL](https://www.postgresql.org/), as it relies on table partitioning,
full-text search and specific aggregation functions.

## Migrations

After a database is started, it is required to run migration script.
If database is empty, it creates all the required tables and indexes.
If database is not empty, it will perform database structure upgrade.

Migration script is a thin wrapper around [Alembic cli](https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration),
options and commands are just the same.

:::{warning}
Other containers (consumer, server) should be stopped while running migrations, to prevent interference.
:::

## Partitions

After migrations are performed, it is required to run {ref}`create-partitions-cli` which creates partitions for some tables in the database.
By default, it creates monthly partitions, for current and next month. This can be changed by overriding command args.

This script should run on schedule, depending on partitions granularity.
Scheduling can be done by adding a dedicated entry to [crontab](https://help.ubuntu.com/community/CronHowto).

It's strongly recommended also to add old partitions cleanup script to cron {ref}`cleanup-partitions-cli`.
Scheduling setup is same is for creating of partitions.

## Analytic views

Along with migrations few analytics views are created. These are managed by {ref}`refresh-analytic-views-cli`,
and should be executed by schedule.

## Seeding

By default, database is created with no data. To seed database with some examples, use {ref}`db-seed-cli`.

## Requirements

- PostgreSQL 12 or higher. It is recommended to use latest Postgres version.

## Install & run

### With Docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile analytics,cleanup,seed up -d
  ```

  `docker-compose` will download PostgreSQL image, create container and volume, and then start container.
  Image entrypoint will create database if volume is empty.

  After that, several one-off containers will start:
  : - `db-create-partitions` will create necessary partitions in db.
    - `db-cleanup-partitions` will cleanup old partitions.
    - `db-refresh-views` will refresh analytic views.
    - `db-seed` will seed database with some examples (optional, can be omitted).

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

  ```{eval-rst}
  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 1-69,176
  ```

  ```{eval-rst}
  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 1-5,23
  ```

- Add scripts to crontab:

  ```console
  $ crontab -e
  ```

  ```text
  0 0 * * * docker compose -f "/path/to/docker-compose.yml" start db-create-partitions db-refresh-views db-cleanup-partitions
  ```

### Without Docker

- For installing PostgreSQL, please follow [installation instruction](https://www.postgresql.org/download/).

- Install Python 3.10 or above

- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  $ source /some/.venv/activate
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[postgres]
  ```

- Configure {ref}`Database connection <configuration-database>` using environment variables, e.g. by creating `.env` file:

  ```{code-block} console
  :caption: /some/.env

  $ export DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://data_rentgen:changeme@localhost:5432/data_rentgen
  ```

  And then read values from this file:

  ```console
  $ source /some/.env
  ```

- Run migrations:

  ```console
  $ python -m data_rentgen.db.migrations upgrade head
  ```

  :::{note}
  This command should be executed after each upgrade to new Data.Rentgen version.
  :::

- Create partitions:

  ```console
  $ python -m data_rentgen.db.scripts.create_partitions
  ```

- Create analytic views:

  ```console
  $ python -m data_rentgen.db.scripts.refresh_analytic_views
  ```

- Seed database with example data (optional, can be omitted):

  ```console
  $ python -m data_rentgen.db.scripts.seed
  ```

- Add scripts to crontab:

  ```console
  $ crontab -e
  ```

  ```text
  # read settings from .env file, and run script using a specific venv with all required dependencies
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.create_partitions"
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.cleanup_partitions truncate --keep-after $(date --date='-1year' '+%Y-%m-%d')"
  0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.refresh_analytic_views"
  ```

## See also

```{toctree}
:maxdepth: 1

configuration
create_partitions_cli
cleanup_partitions_cli
refresh_analytic_views_cli
seed_cli
structure
```
