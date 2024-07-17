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

    entity Dataset {
        * id: BigInteger
        ----
        * location_id
        * name
        format
    }

    entity DatasetSymlink {
        * id: BigInteger
        ----
        * from_dataset_id
        * to_dataset_id
        type
    }

    entity Job {
        * id: BigInteger
        ----
        * location_id
        * name
        type
    }

    entity Run {
        * id: UUIDv7
        * created_at: Datetime
        ----
        * job_id
        status
        name
        parent_run_id
        attempt
        persistent_log_url
        running_log_url
        started_at
        started_by_user_id
        start_reason
        ended_at
        end_reason
    }

    entity Operation {
        * id: UUIDv7
        * created_at: Datetime
        ----
        * run_id
        status
        name
        type
        description
        started_at
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
        * created_at: Datetime
        ----
        * operation_id
        * dataset_id
        * type
        schema_id
        num_bytes
        num_rows
        num_files
    }

    Address ||--o{ Location

    Dataset ||--o{ Location
    Dataset |o--o{ Schema

    DatasetSymlink "from_dataset_id" ||--o{ Dataset
    DatasetSymlink "to_dataset_id" ||--o{ Dataset

    Run ||--o{ Job
    Run "started_by_user_id" ||--o{ User
    Run "parent_run_id" |o--o{ Run

    Operation ||--o{ Run

    Interaction ||--o{ Operation
    Interaction ||--o{ Dataset
    Interaction |o--o{ Schema

    @enduml
