.. _architecture:

Architecture
============

.. plantuml::

    @startuml
        title Backend artitecture
        skinparam linetype polyline
        left to right direction

        agent "OpenLineage client" as CLIENT
        actor "User" as USER

        frame "Arrakis" {
            queue "Message broker" as BROKER
            component "Events consumer" as CONSUMER
            database "Database" as DB
            component "REST API server" as SERVER
        }

        [CLIENT] --> [BROKER]

        [BROKER] --> [CONSUMER]
        [CONSUMER] --> [DB]

        [USER] --> [SERVER]
        [SERVER] --> [DB]

    @enduml
