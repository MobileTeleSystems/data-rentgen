.. _message-broker:

Message Broker
==============

Currently, Data.Rentgen supports only `Apache Kafka <https://kafka.apache.org/>`_ as message broker for raw OpenLineage events.

Requirements
------------

* Apache Kafka 3.x. It is recommended to use latest Kafka version.

Setup
~~~~~

With Docker
^^^^^^^^^^^

* Install `Docker <https://docs.docker.com/engine/install/>`_
* Install `docker-compose <https://github.com/docker/compose/releases/>`_
* Run the following command:

  .. code:: console

      $ docker compose up -d broker

  ``docker-compose`` will download Apache Kafka image, create container and volume, and then start container.

  Image entrypoint will create database if volume is empty.
  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

      .. literalinclude:: ../../../docker-compose.yml
          :emphasize-lines: 43-55,95

  .. dropdown:: ``.env.docker``

      .. literalinclude:: ../../../.env.docker
          :emphasize-lines: 7-20

Without Docker
^^^^^^^^^^^^^^

Please follow `Apacke Kafka installation instruction <https://kafka.apache.org/quickstart#quickstart_startserver>`_.
