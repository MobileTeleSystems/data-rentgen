.. _architecture:

Architecture
============

Components
----------

Data.Rentgen is build using following components:

* `OpenLineage <https://openlineage.io/docs/>`_ clients & integrations with third-party modules (e.g. Apache Spark, Apache Airflow).
* :ref:`message-broker`, receiving events in JSON format.
* :ref:`message-consumer`, parsing JSON messages.
* :ref:`database` for storing consumed & cleaned up data.
* :ref:`server`, serving database data.
* :ref:`frontend`, accessing REST API to navigate created entities & lineage graph.
* :ref:`http2kafka` (optional), proxy for sending OpenLineage events to Kafka using HTTP API.

Architecture diagram
--------------------

.. plantuml::

    @startuml
        title Data.Rentgen artitecture
        skinparam componentStyle rectangle
        left to right direction

        frame "Data.Rentgen" {
            queue "Kafka" as KAFKA
            component "Message consumer" as CONSUMER
            database "PostgreSQL" as DB
            component "REST API server" as API
            component "Frontend" as FRONTEND
            component "HTTP2Kafka" as HTTP2KAFKA
        }

        frame "OpenLineage" {
            agent "OpenLineage Spark" as SPARK
            agent "OpenLineage Airflow" as AIRFLOW
            agent "OpenLineage Hive" as HIVE
            agent "OpenLineage Flink" as FLINK
            agent "OpenLineage dbt" as DBT
            agent "OpenLineage other" as OTHER
            agent "OpenLineage KafkaTransport" as KAFKA_TRANSPORT
            agent "OpenLineage HttpTransport" as HTTP_TRANSPORT
        }

        actor "User" as USER

        [SPARK] --> [KAFKA_TRANSPORT]
        [AIRFLOW] --> [KAFKA_TRANSPORT]
        [HIVE] --> [KAFKA_TRANSPORT]
        [FLINK] --> [KAFKA_TRANSPORT]
        [DBT] --> [KAFKA_TRANSPORT]
        [KAFKA_TRANSPORT] --> [KAFKA]

        [OTHER] --> [HTTP_TRANSPORT]
        [HTTP_TRANSPORT] --> [HTTP2KAFKA]
        [HTTP2KAFKA] --> [KAFKA]

        [KAFKA] --> [CONSUMER]
        [CONSUMER] --> [DB]

        [API] --> [DB]
        [FRONTEND] --> [API]
        [USER] --> [FRONTEND]

    @enduml
