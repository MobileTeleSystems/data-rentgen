.. _database-structure:

Database structure
==================

.. https://plantuml.com/en/ie-diagram

.. plantuml::

    @startuml
    title Database structure

    entity user {
        * id: bigint
        ----
        * name: varchar(256)
    }

    entity address {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * url: varchar(256)
    }

    entity location {
        * id: bigint
        ----
        * type: varchar(32)
        * name: varchar(256)
        external_id: varchar(256) null
        search_vector: tsvector
    }

    entity dataset {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * name: varchar(256)
        format: varchar(256) null
        search_vector: tsvector
    }

    entity dataset_symlink {
        * id: bigint
        ----
        * from_dataset_id: bigint <<FK>>
        * to_dataset_id: bigint <<FK>>
        type: varchar(32)
    }

    entity job {
        * id: bigint
        ----
        * location_id: bigint <<FK>>
        * name: varchar(256)
        type: varchar(32)
        search_vector: tsvector
    }

    entity run {
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

    entity operation {
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

    entity schema {
        * id: bigint
        ----
        * digest: uuid(v5)
        fields: json
    }

    entity input {
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

    entity output {
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

    entity dataset_column_relation {
        * id: bigint
        ----
        * fingerprint: uuid(v5)
        * source_column: varchar(255)
        * target_column: varchar(255) null
        type: smallint
    }

    entity column_lineage {
        * id: uuid(v7)
        * created_at: timestamptz
        ----
        * operation_id: uuid(v7)
        * run_id: uuid(v7)
        * job_id: bigint
        * source_dataset_id: bigint
        * target_dataset_id: bigint
        * fingerprint: uuid(v5)
    }

    address ||--o{ location

    dataset ||--o{ location

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

    ColumnLineage ||--o{ Operation
    ColumnLineage ||--o{ Run
    ColumnLineage ||--o{ Job
    ColumnLineage "source_dataset_id" ||--o{ Dataset
    ColumnLineage "target_dataset_id" ||--o{ Dataset
    ColumnLineage ||--o{ DatasetColumnRelation

    @enduml
