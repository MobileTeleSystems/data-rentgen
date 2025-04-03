.. _overview-install:

Install Data.Rentgen
====================

Requirements
------------

* `Docker <https://docs.docker.com/engine/install/>`_
* `docker-compose <https://github.com/docker/compose/releases/>`_

Install & run
-------------

Copy ``docker-compose.yml`` and ``.env.docker`` from this repo:

.. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../docker-compose.yml

.. dropdown:: ``.env.docker``

    .. literalinclude:: ../../.env.docker


Then start containers using ``docker-compose``:

.. code:: console

    $ docker compose --profile all up -d --wait

``docker-compose`` will download required images, create containers and start them in a proper order.
Options can be set via ``.env.docker`` file or ``environment`` section in ``docker-compose.yml``.

Access Data.Rentgen
^^^^^^^^^^^^^^^^^^^

After all containers are started and ready, you can:

* Browse frontend at http://localhost:3000
* Open REST API Swagger doc at http://localhost:8000/docs
