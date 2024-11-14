.. _database-structure:

Database structure
==================

.. https://plantuml.com/en/ie-diagram

.. plantuml::

    @startumlc
    title Database structure

    entity User {
        * id: bigint
        ----
        * name: varchar(256)
    }

    entity Address {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * url: varchar(256)
    }

    entity Location {
        * id: bigint
        ----
        * type: varchar(32)
        * name: varchar(256)
        external_id: varchar(256) null
        search_vector: tsvector
    }

    entity Dataset {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * name: varchar(256)
        format: varchar(256) null
        search_vector: tsvector
    }

    entity DatasetSymlink {
        * id: bigint
        ----
        * from_dataset_id: bigint <<FK>>
        * to_dataset_id: bigint <<FK>>
        type: varchar(32)
    }

    entity Job {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * name: varchar(256)
        type: varchar(32)
        search_vector: tsvector
    }

    entity Run {
        * id: uuid(v7)
        * created_at: timestamptz
        ----
        * job_id: bigint
        status: smallint
        parent_run_id: bigint null
        started_at: timestamptz null
        started_by_user_id: bigint null
        start_reason: varchar(32)
        ended_at: timestamptz null
        end_reason: text null
        external_id: text null
        attempt: varchar(64) null
        persistent_log_url: timestamptz null
        running_log_url: timestamptz null
        search_vector: tsvector
    }

    entity Operation {
        * id: uuid(v7)
        * created_at: timestamptz
        ----
        * run_id: uuid(v7)
        status: smallint
        name: text
        type: varchar(32)
        position: int null
        group: text null
        description: text null
        started_at: timestamptz null
        ended_at: timestamptz null
    }

    entity Schema {
        * id: bigint
        ----
        * digest: uuid(v5)
        fields: json
    }

    entity Input {
        * id: uuid(v7)
        * created_at: timestamptz
        ----
        * operation_id: uuid(v7)
        * run_id: uuid(v7)
        * job_id: bigint
        * dataset_id: bigint
        schema_id: bigint null
        num_bytes: bigint
        num_rows: bigint
        num_files: bigint
    }

    entity Output {
        * id: uuid(v7)
        * created_at: timestamptz
        ----
        * operation_id: uuid(v7)
        * run_id: uuid(v7)
        * job_id: bigint
        * dataset_id: bigint
        * type: varchar(32)
        schema_id: bigint null
        num_bytes: bigint
        num_rows: bigint
        num_files: bigint
    }

    Address ||--o{ Location

    Dataset ||--o{ Location

    DatasetSymlink "from_dataset_id" ||--o{ Dataset
    DatasetSymlink "to_dataset_id" ||--o{ Dataset

    Run ||--o{ Job
    Run "started_by_user_id" ||--o{ User
    Run "parent_run_id" |o--o{ Run

    Operation ||--o{ Run

    Input ||--o{ Operation
    Input ||--o{ Run
    Input ||--o{ Job
    Input ||--o{ Dataset
    Input |o--o{ Schema

    Output ||--o{ Operation
    Output ||--o{ Run
    Output ||--o{ Job
    Output ||--o{ Dataset
    Output |o--o{ Schema

    @enduml
