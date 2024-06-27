.. _backend-architecture:

Architecture
============

.. plantuml::

    @startuml
        title Backend artitecture
        skinparam linetype polyline
        left to right direction

        agent "ETL Process"
        actor "User"

        frame "Arrakis" {
            queue "Message broker"
            component "Message consumer"
            database "Database"
            component "REST API"
        }

        [ETL Process] --> [Message broker]

        [Message broker] --> [Message consumer]
        [Message consumer] --> [Database]

        [User] --> [REST API]
        [REST API] --> [Database]

    @enduml
