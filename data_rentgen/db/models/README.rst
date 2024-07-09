DB structure
============

.. https://plantuml.com/ru/ie-diagram

.. plantuml::

    @startuml
    title Database structure

    left to right direction

    entity User {
        * id: BigInteger
        ----
        * name
    }

    entity Address {
        * id: BigInteger
        ----
        * location_id
        * url
    }

    entity Location {
        * id: BigInteger
        ----
        * type
        * name
    }

    entity Storage {
        * id: BigInteger
        ----
        * location_id
        name
    }

    entity Dataset {
        * id: BigInteger
        ----
        * storage_id
        * name
        format
        alias_for_dataset_id
    }

    entity Job {
        * id: BigInteger
        ----
        * location_id
        * name
    }

    entity Runner {
        * id: BigInteger
        ----
        * location_id
        * type
        version
    }

    entity Run {
        * id: UUIDv7
        * started_at: Datetime
        ----
        * job_id
        runner_id
        status
        name
        parent_run_id
        attempt
        description
        log_url
        started_by_user_id
        ended_at
        ended_reason
    }

    entity Operation {
        * id: UUIDv7
        * started_at: Datetime
        ----
        * run_id
        status
        name
        type
        description
        ended_at
    }

    entity Schema {
        * id: BigInt
        ----
        * digest: UUIDv5
        fields: JSON
    }

    entity Interaction {
        * id: UUIDv7
        * started_at: Datetime
        ----
        * operation_id
        * dataset_id
        type
        ended_at
        schema_id
        connect_as_user_id
        num_bytes
        num_rows
        num_files
    }

    Address ||--o{ Location
    Storage ||--o{ Location

    Dataset ||--o{ Storage
    Dataset "alias_of_dataset_id" |o--o{ Dataset

    Run ||--o{ Job
    Run |o--o{ Runner
    Run "started_by_user_id" ||--o{ User
    Run "parent_run_id" |o--o{ Run

    Operation ||--o{ Run

    Interaction ||--o{ Operation
    Interaction ||--o{ Dataset
    Interaction |o--o{ Schema
    Interaction "connect_as_user_id" |o--o{ User

    @enduml
