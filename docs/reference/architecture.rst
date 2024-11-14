.. _architecture:

Architecture
============

Components
----------

Data.Rentgen is build using following components:

* `OpenLineage <https://openlineage.io/docs/>`_ clients & integrations with third-party modules (e.g. Apache Spark, Apache Airflow)
* :ref:`message-broker`, receiving events in JSON format
* :ref:`message-consumer`, parsing JSON messages
* :ref:`database` for storing consumed & cleaned up data
* :ref:`server`, serving database data
* :ref:`frontend`, accessing REST API to navigate created entities & lineage graph

Architecture diagram
--------------------

.. plantuml::

    @startuml
        title Data.Rentgen artitecture
        skinparam componentStyle rectangle
        left to right direction

        agent "OpenLineage Spark integration" as SPARK
        agent "OpenLineage Airflow integration" as AIRFLOW
        agent "OpenLineage Client" as OPENLINEAGE

        frame "Data.Rentgen" {
            queue "Kafka" as KAFKA
            component "Message consumer" as CONSUMER
            database "PostgreSQL" as DB
            component "REST API server" as API
            component "Frontend" as FRONTEND
        }

        actor "User" as USER

        [SPARK] --> [KAFKA]
        [AIRFLOW] --> [KAFKA]
        [OPENLINEAGE] --> [KAFKA]

        [KAFKA] --> [CONSUMER]
        [CONSUMER] --> [DB]

        [API] --> [DB]
        [FRONTEND] --> [API]
        [USER] --> [FRONTEND]

    @enduml
