.. _install-server:

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

Docker will download backend image of Postgres and REST API server, and run them.
Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

.. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 1-32,56-57

.. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 1-4,21-26

After containers are ready, open http://localhost:8000/docs.

Without docker
--------------

Requirements
~~~~~~~~~~~~

* Python 3.10 or above
* Some relation database instance, like `Postgres <https://www.postgresql.org/>`_

Installation process
~~~~~~~~~~~~~~~~~~~~

Install ``data-rentgen`` package with following *extra* dependencies:

.. code-block:: console

    $ pip install data-rentgen[server,postgres]

Available *extras* are:

* ``server`` - REST API server requirements, like FastAPI and so on.
* ``postgres`` - requirements required to use Postgres as data storage.

Run database
~~~~~~~~~~~~

Start Postgres instance somewhere, and set up database url using environment variables:

.. code-block:: bash

    DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://user:password@postgres-host:5432/database_name

You can use virtually any database supported by `SQLAlchemy <https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls>`_,
but the only one we really tested is Postgres.

See :ref:`Database settings <configuration-server-database>` for more options.

Run migrations
~~~~~~~~~~~~~~

To apply migrations (database structure changes) you need to execute following command:

.. code-block:: console

    $ python -m data_rentgen.db.migrations upgrade head

This is a thin wrapper around `alembic <https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration>`_ cli,
options and commands are just the same.

.. note::

    This command should be executed after each upgrade to new DataRentgen version.

Run REST API server
~~~~~~~~~~~~~~~~~~~

To start REST API server you need to execute following command:

.. code-block:: console

    $ python -m data_rentgen.server --host 0.0.0.0 --port 8000

This is a thin wrapper around `uvicorn <https://www.uvicorn.org/#command-line-options>`_ cli,
options and commands are just the same.

After server is started and ready, open http://localhost:8000/docs.
