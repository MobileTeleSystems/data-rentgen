.. _message-broker:

Message Broker
==============

Message broker is component used by OpenLineage to store data, and then read by :ref:`message-consumer` in batches.

Currently, Data.Rentgen supports only `Apache Kafka <https://kafka.apache.org/>`_ as message broker.

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

    $ docker compose --profile broker up -d --wait

  ``docker-compose`` will download Apache Kafka image, create container and volume, and then start container.

  Image entrypoint will create database if volume is empty.
  Options can be set via ``.env`` file or ``environment`` section in ``docker-compose.yml``

  .. dropdown:: ``docker-compose.yml``

    .. literalinclude:: ../../../docker-compose.yml
        :emphasize-lines: 65-81,124

  .. dropdown:: ``.env.docker``

    .. literalinclude:: ../../../.env.docker
        :emphasize-lines: 7-20

Without Docker
^^^^^^^^^^^^^^

Please follow `Apache Kafka installation instruction <https://kafka.apache.org/quickstart#quickstart_startserver>`_.
