.. _server:

REST API Server
===============

Data.Rentgen REST API server provides simple HTTP API for accessing entities stored in :ref:`database`.
Implemented using `FastAPI <https://fastapi.tiangolo.com/>`_.

Install & run
-------------

With docker
^^^^^^^^^^^

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_

* Run the following command:

  .. code:: console

    $ docker compose up -d db server

  ``docker-compose`` will download all necessary images, create containers, and then start the server.

  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

      .. literalinclude:: ../../../docker-compose.yml
          :emphasize-lines: 17-41

  .. dropdown:: ``.env.docker``

      .. literalinclude:: ../../../.env.docker
          :emphasize-lines: 22-27

* After server is started and ready, open http://localhost:8000/docs.

Without docker
^^^^^^^^^^^^^^

* Install Python 3.10 or above
* Setup :ref:`database`, run migrations and create partitions
* Create virtual environment

  .. code-block:: console

      $ python -m venv /some/.venv
      $ source /some/.venv/activate

* Install ``data-rentgen`` package with following *extra* dependencies:

  .. code-block:: console

      $ pip install data-rentgen[server,postgres]

* Run server process

  .. code-block:: console

      $ python -m data_rentgen.server --host 0.0.0.0 --port 8000

  This is a thin wrapper around `uvicorn <https://www.uvicorn.org/#command-line-options>`_ cli,
  options and commands are just the same.

* After server is started and ready, open http://localhost:8000/docs.

See also
--------

.. toctree::
    :maxdepth: 1

    configuration/index
    openapi
