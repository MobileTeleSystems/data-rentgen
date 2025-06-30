.. _database:

Relation Database
=================

Data.Rentgen uses relational database as a storage for lineage entities and relations.

Currently, Data.Rentgen supports only `PostgreSQL <https://www.postgresql.org/>`_, as it relies on table partitioning,
full-text search and specific aggregation functions.

Migrations
----------

After a database is started, it is required to run migration script.
For empty database, it creates all the required tables and indexes.
For non-empty database, it will perform database structure upgrade.

Migration script is a thin wrapper around `Alembic cli <https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration>`_,
options and commands are just the same.

.. warning::

    Other containers (consumer, server) should be stopped while running migrations, to prevent interference.

Partitions
---------------

After migrations are performed, it is required to run :ref:`create-partitions-cli` which creates partitions for some tables in the database.
By default, it creates monthly partitions, for current and next month. This can be changed by overriding command args.

This script should run on schedule, depending on partitions granularity.
Scheduling can be done by adding a dedicated entry to `crontab <https://help.ubuntu.com/community/CronHowto>`_.


It's strongly recommended also to setup cleaning data in old partitions :ref:`clean-partitions-cli`.
Scheduling setup is same is for creating of partitions.

Analytic views
---------------

Along with migrations few analytics views are created. These are managed by :ref:`refres-analytic-views-cli`,
and should be executed by schedule.

Requirements
------------

* PostgreSQL 12 or higher. It is recommended to use latest Postgres version.

Install & run
-------------

With Docker
~~~~~~~~~~~

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_

* Run the following command:

  .. code:: console

    $ docker compose up -d db db-migrations

  ``docker-compose`` will download PostgreSQL image, create container and volume, and then start container.
  Image entrypoint will create database if volume is empty.

  After that, one-off container with migrations & partitions creation script will run, and perform all necessary steps.

  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 1-33,123

  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 1-5,23

* Create partitions:

  .. code:: console

    $ docker exec data-rentgen-server-1 "python -m data_rentgen.db.scripts.create_partitions"

* Add partitions creation script to crontab:

  .. code:: console

    $ crontab -e

  .. code:: text

    0 0 * * * docker exec data-rentgen-server-1 "python -m data_rentgen.db.scripts.create_partitions"

* Create analytic views:

  .. code:: console

    $ docker exec data-rentgen-server-1 "python -m data_rentgen.db.scripts.refresh_analytic_views"

* Add analytic views refresh script to crontab:

  .. code:: console

    $ crontab -e

  .. code:: text

    0 0 * * * docker exec data-rentgen-server-1 "python -m data_rentgen.db.scripts.refresh_analytic_views"

* Add cleaning partitions script to crontab:

.. code:: console

    $ crontab -e

  .. code:: text

    0 0 * * * docker exec data-rentgen-server-1 "python -m data_rentgen.db.scripts.clean_partitions command truncate --keep-after $(date -v2m '+%Y-%m-%d')"



Without Docker
~~~~~~~~~~~~~~

* For installing PostgreSQL, please follow `installation instruction <https://www.postgresql.org/download/>`_.
* Install Python 3.10 or above
* Create virtual environment

  .. code-block:: console

      $ python -m venv /some/.venv
      $ source /some/.venv/activate

* Install ``data-rentgen`` package with following *extra* dependencies:

  .. code-block:: console

      $ pip install data-rentgen[postgres]

* Configure :ref:`Database connection <configuration-database>` using environment variables, e.g. by creating ``.env`` file:

  .. code-block:: console
    :caption: /some/.env

    $ export DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://data_rentgen:changeme@localhost:5432/data_rentgen

  And then read values from this file:

  .. code:: console

    $ source /some/.env

* Run migrations:

  .. code:: console

    $ python -m data_rentgen.db.migrations upgrade head

  .. note::

      This command should be executed after each upgrade to new Data.Rentgen version.

* Create partitions:

  .. code:: console

    $ python -m data_rentgen.db.scripts.create_partitions

* Add partitions creation script to crontab, to run every day:

  .. code:: console

    $ crontab -e

  .. code:: text

    # read settings from .env file, and run script using a specific venv with all required dependencies
    0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.create_partitions"

* Create analytic views:

  .. code:: console

    $ python -m data_rentgen.db.scripts.refresh_analytic_views

* Add analytic views refresh script to crontab:

  .. code:: console

    $ crontab -e

  .. code:: text

    # read settings from .env file, and run script using a specific venv with all required dependencies
    0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.refresh_analytic_views"

* Add partitions cleaning script to crontab, to run every day:

  .. code:: console

    $ crontab -e

  .. code:: text

    # read settings from .env file, and run script using a specific venv with all required dependencies
    0 0 * * * /bin/bash -c "source /some/.env && /some/.venv/bin/python -m data_rentgen.db.scripts.clean_partitions command truncate --keep-after $(date -v2m '+%Y-%m-%d')"


See also
--------

.. toctree::
    :maxdepth: 1

    configuration
    partitions_cli
    analytic_views_cli
    clean_data_cli
    structure
