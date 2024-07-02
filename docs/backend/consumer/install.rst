.. _install-consumer:

Install & run
=============

With docker
-----------

Requirements
~~~~~~~~~~~~

* `Docker <https://docs.docker.com/engine/install/>`_
* `docker-compose <https://github.com/docker/compose/releases/>`_

Installation process
~~~~~~~~~~~~~~~~~~~~

Docker will download backend image of Kafka, POstgres and Arrakis Kafka consumer, and run them.
Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

.. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 1-15,34-

.. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker

Without docker
--------------

Requirements
~~~~~~~~~~~~

* Python 3.10 or above
* Some relation database instance, like `Postgres <https://www.postgresql.org/>`_

Installation process
~~~~~~~~~~~~~~~~~~~~

Install ``data-arrakis`` package with following *extra* dependencies:

.. code-block:: console

    $ pip install data-arrakis[consumer,postgres]

Available *extras* are:

* ``consumer`` - Kafka consumer requirements, like FastStream and so on.
* ``postgres`` - requirements required to use Postgres as data storage.

Run database
~~~~~~~~~~~~

Start Postgres instance somewhere, and set up database url using environment variables:

.. code-block:: bash

    ARRAKIS__DATABASE__URL=postgresql+asyncpg://user:password@postgres-host:5432/database_name

You can use virtually any database supported by `SQLAlchemy <https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls>`_,
but the only one we really tested is Postgres.

See :ref:`Database settings <configuration-server-database>` for more options.

Run migrations
~~~~~~~~~~~~~~

To apply migrations (database structure changes) you need to execute following command:

.. code-block:: console

    $ python -m arrakis.db.migrations upgrade head

This is a thin wrapper around `alembic <https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration>`_ cli,
options and commands are just the same.

.. note::

    This command should be executed after each upgrade to new Arrakis version.

Run Kafka
~~~~~~~~~

Start Kafka instance somewhere, and set up connection options using environment variables:

.. code-block:: bash

    ARRAKIS__KAFKA__BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
    ARRAKIS__KAFKA__SECURITY__TYPE=scram-sha256
    ARRAKIS__KAFKA__SECURITY__USER=user
    ARRAKIS__KAFKA__SECURITY__PASSWORD=password

See :ref:`Kafka settings <configuration-kafka>` for more options.

Run Kafka consumer
~~~~~~~~~~~~~~~~~~

To start Kafka consumer, you need to execute following command:

.. code-block:: console

    $ python -m arrakis.consumer

This is a thin wrapper around `FastStream <https://faststream.airt.ai/latest/getting-started/cli/>`_ cli,
options and commands are just the same.
