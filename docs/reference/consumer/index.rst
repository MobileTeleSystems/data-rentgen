.. _message-consumer:

Message Consumer
================

Data.Rentgen fetches messages from a :ref:`message-broker` using a `FastStream <https://faststream.airt.ai>`_ based consumer, parses incoming messages, and creates all parsed entities in the :ref:`database`.

Install & run
-------------

With docker
^^^^^^^^^^^

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_

* Run the following command:

  .. code:: console

      $ docker compose up -d db consumer

  ``docker-compose`` will download all necessary images, create containers, and then start consumer process.

  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

      .. literalinclude:: ../../../docker-compose.yml
          :emphasize-lines: 57-66

  .. dropdown:: ``.env.docker``

      .. literalinclude:: ../../../.env.docker
          :emphasize-lines: 22-24,29-34

Without docker
^^^^^^^^^^^^^^

* Install Python 3.10 or above
* Setup :ref:`database`, run migrations and create partitions
* Setup :ref:`message-broker`
* Create virtual environment

  .. code-block:: console

      $ python -m venv /some/.venv
      $ source /some/.venv/activate

* Install ``data-rentgen`` package with following *extra* dependencies:

  .. code-block:: console

      $ pip install data-rentgen[consumer,postgres]

* Run consumer process

  .. code-block:: console

      $ python -m data_rentgen.consumer

See also
--------

.. toctree::
    :maxdepth: 1

    configuration/index
