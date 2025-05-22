from datetime import datetime, timezone

from pydantic import TypeAdapter
from uuid6 import UUID

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageDatasetFacets,
    OpenLineageDatasourceDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.documentation import OpenLineageDocumentationDatasetFacet
from data_rentgen.consumer.openlineage.dataset_facets.schema import (
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
    OpenLineageSqlJobFacet,
)
from data_rentgen.consumer.openlineage.run import OpenLineageRun
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


def test_run_event_dbt_job_start():
    json = {
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": "2025-05-20T08:26:55.524789+00:00",
        "eventType": "START",
        "job": {
            "namespace": "local://somehost",
            "name": "dbt-run-demo_project",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "DBT",
                    "jobType": "JOB",
                    "processingType": "BATCH",
                },
            },
        },
        "run": {
            "runId": "0196eccd-8aa4-7274-9116-824575596aaf",
            "facets": {},
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 5, 20, 8, 26, 55, 524789, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_dbt_job_end():
    json = {
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": "2025-05-20T08:27:20.413075+00:00",
        "eventType": "COMPLETE",
        "job": {
            "namespace": "local://somehost",
            "name": "dbt-run-demo_project",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "DBT",
                    "jobType": "JOB",
                    "processingType": "BATCH",
                },
            },
        },
        "run": {
            "runId": "0196eccd-8aa4-7274-9116-824575596aaf",
            "facets": {},
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_dbt_model_start():
    json = {
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": "2025-05-20T08:27:16.601799Z",
        "eventType": "START",
        "job": {
            "namespace": "local://somehost",
            "name": "demo_schema.demo_project.target_table",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "DBT",
                    "jobType": "MODEL",
                    "processingType": "BATCH",
                },
                "sql": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                    "query": "\n\nselect\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
                },
            },
        },
        "run": {
            "runId": "0196eccd-ebdc-70e3-b532-6e66f541ba29",
            "facets": {
                "dbt_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/schema/dbt-version-run-facet.json",
                    "version": "1.9.4",
                },
                "parent": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "job": {
                        "name": "dbt-run-demo_project",
                        "namespace": "local://somehost",
                    },
                    "run": {
                        "runId": "0196eccd-8aa4-7274-9116-824575596aaf",
                    },
                },
            },
        },
        "inputs": [
            {
                "name": "demo_schema.source_table",
                "namespace": "spark://localhost:10000",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "description": "The primary key for this table",
                                "fields": [],
                                "name": "id",
                            },
                        ],
                    },
                },
            },
        ],
        "outputs": [
            {
                "name": "demo_schema.target_table",
                "namespace": "spark://localhost:10000",
                "facets": {
                    "columnLineage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                        "dataset": [],
                        "fields": {
                            "complex_id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "complex_id",
                                        "transformations": [],
                                    },
                                ],
                            },
                            "day": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                            },
                            "id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                            },
                        },
                    },
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                },
                "outputFacets": {},
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 5, 20, 8, 27, 16, 601799, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n"
                        "\nselect"
                        "\nid,"
                        "\ncomplex_id,"
                        "\n2023 as year,"
                        "\n10 as month,"
                        "\nid as day"
                        "\nfrom demo_schema.source_table"
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="dbt-run-demo_project",
                        namespace="local://somehost",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="spark://localhost:10000",
                name="demo_schema.source_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="id", description="The primary key for this table", fields=[]),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="spark://localhost:10000",
                name="demo_schema.target_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "complex_id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="complex_id",
                                    ),
                                ],
                            ),
                            "day": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                            ),
                            "id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                            ),
                        },
                    ),
                ),
            ),
        ],
    )


def test_run_event_dbt_model_complete():
    json = {
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": "2025-05-20T08:27:18.581235Z",
        "eventType": "COMPLETE",
        "job": {
            "namespace": "local://somehost",
            "name": "demo_schema.demo_project.target_table",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/dbt",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "DBT",
                    "jobType": "MODEL",
                    "processingType": "BATCH",
                },
                "sql": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                    "query": "\n\nselect\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
                },
            },
        },
        "run": {
            "runId": "0196eccd-ebdc-70e3-b532-6e66f541ba29",
            "facets": {
                "dbt_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/schema/dbt-version-run-facet.json",
                    "version": "1.9.4",
                },
                "parent": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "job": {
                        "name": "dbt-run-demo_project",
                        "namespace": "local://somehost",
                    },
                    "run": {
                        "runId": "0196eccd-8aa4-7274-9116-824575596aaf",
                    },
                },
            },
        },
        "inputs": [
            {
                "name": "demo_schema.source_table",
                "namespace": "spark://localhost:10000",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "id",
                                "description": "The primary key for this table",
                                "fields": [],
                            },
                        ],
                    },
                },
            },
        ],
        "outputs": [
            {
                "name": "demo_schema.target_table",
                "namespace": "spark://localhost:10000",
                "facets": {
                    "columnLineage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                        "dataset": [],
                        "fields": {
                            "complex_id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "complex_id",
                                        "transformations": [],
                                    },
                                ],
                            },
                            "day": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                            },
                            "id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                            },
                        },
                    },
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.33.0/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                },
                "outputFacets": {},
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 5, 20, 8, 27, 18, 581235, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n"
                        "\nselect"
                        "\nid,"
                        "\ncomplex_id,"
                        "\n2023 as year,"
                        "\n10 as month,"
                        "\nid as day"
                        "\nfrom demo_schema.source_table"
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="dbt-run-demo_project",
                        namespace="local://somehost",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="spark://localhost:10000",
                name="demo_schema.source_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="id", description="The primary key for this table", fields=[]),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="spark://localhost:10000",
                name="demo_schema.target_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "complex_id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="complex_id",
                                    ),
                                ],
                            ),
                            "day": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                            ),
                            "id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                            ),
                        },
                    ),
                ),
            ),
        ],
    )


def test_run_event_dbt_model_start_before_openlineage_1_33():
    # Fixed by https://github.com/OpenLineage/OpenLineage/pull/3700 and https://github.com/OpenLineage/OpenLineage/pull/3707
    json = {
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/integration/dbt",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventTime": "2025-05-20T08:27:16.601799Z",
        "eventType": "START",
        "job": {
            "namespace": "local://somehost",
            "name": "demo_schema.demo_project.target_table",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/integration/dbt",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "DBT",
                    "jobType": "MODEL",
                    "processingType": "BATCH",
                },
                "sql": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                    "query": "\n\nselect\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
                },
            },
        },
        "run": {
            "runId": "0196eccd-ebdc-70e3-b532-6e66f541ba29",
            "facets": {
                "dbt_version": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/schema/dbt-version-run-facet.json",
                    "version": "1.9.4",
                },
                "parent": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "job": {
                        "name": "dbt-run-demo_project",
                        "namespace": "local://somehost",
                    },
                    "run": {
                        "runId": "0196eccd-8aa4-7274-9116-824575596aaf",
                    },
                },
            },
        },
        "inputs": [
            {
                "namespace": "spark://localhost:10000",
                "name": "None.demo_schema.source_table",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "description": "The primary key for this table",
                                "fields": [],
                                "name": "id",
                                "type": "",
                            },
                        ],
                    },
                },
            },
        ],
        "outputs": [
            {
                "namespace": "spark://localhost:10000",
                "name": "None.demo_schema.target_table",
                "facets": {
                    "columnLineage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                        "dataset": [],
                        "fields": {
                            "complex_id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "None.demo_schema.source_table",
                                        "field": "complex_id",
                                        "transformations": [],
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                            "day": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "None.demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                            "id": {
                                "inputFields": [
                                    {
                                        "namespace": "spark://localhost:10000",
                                        "name": "None.demo_schema.source_table",
                                        "field": "id",
                                        "transformations": [],
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                        },
                    },
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "spark://localhost:10000",
                        "uri": "spark://localhost:10000",
                    },
                    "documentation": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet",
                        "description": "",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.1/client/python",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [],
                    },
                },
                "outputFacets": {},
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2025, 5, 20, 8, 27, 16, 601799, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n"
                        "\nselect"
                        "\nid,"
                        "\ncomplex_id,"
                        "\n2023 as year,"
                        "\n10 as month,"
                        "\nid as day"
                        "\nfrom demo_schema.source_table"
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="dbt-run-demo_project",
                        namespace="local://somehost",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="spark://localhost:10000",
                name="None.demo_schema.source_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    documentation=OpenLineageDocumentationDatasetFacet(
                        description="",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="id",
                                type="",
                                description="The primary key for this table",
                                fields=[],
                            ),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="spark://localhost:10000",
                name="None.demo_schema.target_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="spark://localhost:10000",
                        uri="spark://localhost:10000",
                    ),
                    documentation=OpenLineageDocumentationDatasetFacet(
                        description="",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[],
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "complex_id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="None.demo_schema.source_table",
                                        field="complex_id",
                                    ),
                                ],
                                transformationDescription="",
                                transformationType="",
                            ),
                            "day": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="None.demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                                transformationDescription="",
                                transformationType="",
                            ),
                            "id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="spark://localhost:10000",
                                        name="None.demo_schema.source_table",
                                        field="id",
                                    ),
                                ],
                                transformationDescription="",
                                transformationType="",
                            ),
                        },
                    ),
                ),
            ),
        ],
    )
