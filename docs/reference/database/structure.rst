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
        * created_at: timestamptz
        * id: uuid(v7)
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

    entity sql_query {
        * id: bigint
        ----
        * fingerprint: uuid(v5)
        query: text
    }

    entity operation {
        * created_at: timestamptz
        * id: uuid(v7)
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
        sql_query_id: bigint null
    }

    entity schema {
        * id: bigint
        ----
        * digest: uuid(v5)
        fields: json
    }

    entity input {
        * created_at: timestamptz
        * id: uuid(v7)
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
        * created_at: timestamptz
        * id: uuid(v7)
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
        * created_at: timestamptz
        * id: uuid(v7)
        ----
        * operation_id: uuid(v7)
        * run_id: uuid(v7)
        * job_id: bigint
        * source_dataset_id: bigint
        * target_dataset_id: bigint
        fingerprint: uuid(v5)
    }

    address ||--o{ location

    dataset ||--o{ location
    job ||--o{ location

    dataset_symlink "from_dataset_id" ||--o{ dataset
    dataset_symlink "to_dataset_id" ||--o{ dataset

    run ||--o{ job
    run "started_by_user_id" ||--o{ user
    run "parent_run_id" |o--o{ run

    operation ||--o{ run
    operation |o--o{ sql_query

    input ||--o{ operation
    input ||--o{ run
    input ||--o{ job
    input ||--o{ dataset
    input |o--o{ schema

    output ||--o{ operation
    output ||--o{ run
    output ||--o{ job
    output ||--o{ dataset
    output |o--o{ schema

    column_lineage ||--o{ operation
    column_lineage ||--o{ run
    column_lineage ||--o{ job
    column_lineage "source_dataset_id" ||--o{ dataset
    column_lineage "target_dataset_id" ||--o{ dataset
    column_lineage "fingerprint" ||--o{ dataset_column_relation

    @enduml
