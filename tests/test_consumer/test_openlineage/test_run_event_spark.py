from datetime import datetime, timezone

from packaging.version import Version
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
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
    OpenLineageDatasetFacets,
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageDatasourceDatasetFacet,
    OpenLineageLifecycleStateChangeDatasetFacet,
    OpenLineageOutputDatasetFacets,
    OpenLineageOutputStatisticsOutputDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobIntegrationType,
    OpenLineageJobProcessingType,
    OpenLineageJobType,
    OpenLineageJobTypeJobFacet,
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
    OpenLineageProcessingEngineName,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
    OpenLineageSparkJobDetailsRunFacet,
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


def test_run_event_spark_application_start():
    json = {
        "eventTime": "2024-07-05T09:04:48.7949Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "START",
        "run": {
            "runId": "01908224-8410-79a2-8de6-a769ad6944c9",
            "facets": {
                "spark_properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "properties": {
                        "spark.master": "local[*]",
                        "spark.app.name": "spark_session",
                    },
                },
                "processing_engine": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
                    "version": "3.4.3",
                    "name": "spark",
                    "openlineageAdapterVersion": "1.18.0",
                },
                "spark_applicationDetails": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "master": "local[*]",
                    "appName": "spark_session",
                    "applicationId": "local-1719136537510",
                    "deployMode": "client",
                    "driverHost": "127.0.0.1",
                    "userName": "myuser",
                    "uiWebUrl": "http://127.0.0.1:4040",
                },
                "environment-properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "environment-properties": {},
                },
            },
        },
        "job": {
            "namespace": "spark_integration",
            "name": "spark_session",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "processingType": "NONE",
                    "integration": "SPARK",
                    "jobType": "APPLICATION",
                },
            },
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="spark_integration",
            name="spark_session",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.APPLICATION,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name=OpenLineageProcessingEngineName.SPARK,
                    openlineageAdapterVersion=Version("1.18.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[*]",
                    appName="spark_session",
                    applicationId="local-1719136537510",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="127.0.0.1",
                    userName="myuser",
                    uiWebUrl="http://127.0.0.1:4040",
                ),
            ),
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_spark_application_stop():
    json = {
        "eventTime": "2024-07-05T09:07:15.646Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "COMPLETE",
        "run": {
            "runId": "01908224-8410-79a2-8de6-a769ad6944c9",
            "facets": {
                "spark_properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "properties": {
                        "spark.master": "local[*]",
                        "spark.app.name": "spark_session",
                    },
                },
                "processing_engine": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
                    "version": "3.4.3",
                    "name": "spark",
                    "openlineageAdapterVersion": "1.18.0",
                },
                "environment-properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "environment-properties": {},
                },
            },
        },
        "job": {
            "namespace": "spark_integration",
            "name": "spark_session",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "processingType": "NONE",
                    "integration": "SPARK",
                    "jobType": "APPLICATION",
                },
            },
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="spark_integration",
            name="spark_session",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.APPLICATION,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name=OpenLineageProcessingEngineName.SPARK,
                    openlineageAdapterVersion=Version("1.18.0"),
                ),
            ),
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_spark_job_running():
    json = {
        "eventTime": "2024-07-05T09:07:09.849Z",
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "RUNNING",
        "run": {
            "runId": "01908225-1fd7-746b-910c-70d24f2898b1",
            "facets": {
                "parent": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "run": {"runId": "01908224-8410-79a2-8de6-a769ad6944c9"},
                    "job": {"namespace": "spark_integration", "name": "spark_session"},
                },
                "spark_properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "properties": {
                        "spark.master": "local[*]",
                        "spark.app.name": "spark_session",
                    },
                },
                "processing_engine": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
                    "version": "3.4.3",
                    "name": "spark",
                    "openlineageAdapterVersion": "1.18.0",
                },
                "spark_jobDetails": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "jobId": 3,
                    "jobDescription": "Hive -> Clickhouse",
                },
                "environment-properties": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "environment-properties": {},
                },
            },
        },
        "job": {
            "namespace": "spark_integration",
            "name": "spark_session.execute_save_into_data_source_command",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "processingType": "BATCH",
                    "integration": "SPARK",
                    "jobType": "SQL_JOB",
                },
            },
        },
        "inputs": [
            {
                "namespace": "hdfs://test-hadoop:9820",
                "name": "/user/hive/warehouse/mydatabase.db/source_table",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "hdfs://test-hadoop:9820",
                        "uri": "hdfs://test-hadoop:9820",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "dt",
                                "type": "timestamp",
                                "description": "Business date",
                            },
                            {"name": "customer_id", "type": "decimal(20,0)"},
                            {"name": "total_spent", "type": "float"},
                        ],
                    },
                    "symlinks": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                        "identifiers": [
                            {
                                "namespace": "hive://test-hadoop:9083",
                                "name": "mydatabase.source_table",
                                "type": "TABLE",
                            },
                        ],
                    },
                },
                "inputFacets": {},
            },
        ],
        "outputs": [
            {
                "namespace": "clickhouse://localhost:8123",
                "name": "mydb.myschema.mytable",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "clickhouse://localhost:8123",
                        "uri": "clickhouse://localhost:8123",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {"name": "dt", "type": "timestamp"},
                            {"name": "customer_id", "type": "decimal(20,0)"},
                            {"name": "total_spent", "type": "float"},
                        ],
                    },
                    "columnLineage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                        "fields": {
                            "dt": {
                                "inputFields": [
                                    {
                                        "namespace": "hdfs://test-hadoop:9820",
                                        "name": "/user/hive/warehouse/mydatabase.db/source_table",
                                        "field": "dt",
                                        "transformations": [
                                            {
                                                "type": "DIRECT",
                                                "subtype": "IDENTITY",
                                                "description": "",
                                                "masking": False,
                                            },
                                        ],
                                    },
                                ],
                            },
                            "customer_id": {
                                "inputFields": [
                                    {
                                        "namespace": "hdfs://test-hadoop:9820",
                                        "name": "/user/hive/warehouse/mydatabase.db/source_table",
                                        "field": "customer_id",
                                        "transformations": [
                                            {
                                                "type": "DIRECT",
                                                "subtype": "IDENTITY",
                                                "description": "",
                                                "masking": False,
                                            },
                                        ],
                                    },
                                ],
                            },
                            "total_spent": {
                                "inputFields": [
                                    {
                                        "namespace": "hdfs://test-hadoop:9820",
                                        "name": "/user/hive/warehouse/mydatabase.db/source_table",
                                        "field": "total_spent",
                                        "transformations": [
                                            {
                                                "type": "DIRECT",
                                                "subtype": "IDENTITY",
                                                "description": "",
                                                "masking": False,
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                        "dataset": [
                            {
                                "namespace": "hdfs://test-hadoop:9820",
                                "name": "/user/hive/warehouse/mydatabase.db/source_table",
                                "field": "customer_id",
                                "transformations": [
                                    {
                                        "type": "INDIRECT",
                                        "subtype": "JOIN",
                                        "description": "ON (DISCOUNTS.CUSTOMERS_ID=CUSTOMERS.ID)",
                                    },
                                ],
                            },
                        ],
                    },
                    "lifecycleStateChange": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                        "lifecycleStateChange": "OVERWRITE",
                    },
                },
                "outputFacets": {
                    "outputStatistics": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.18.0/integration/spark",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
                        "rowCount": 10000,
                        "size": 5000000,
                    },
                },
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 7, 9, 849000, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.RUNNING,
        job=OpenLineageJob(
            namespace="spark_integration",
            name="spark_session.execute_save_into_data_source_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.JOB,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908225-1fd7-746b-910c-70d24f2898b1"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="spark_integration",
                        name="spark_session",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
                    ),
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name=OpenLineageProcessingEngineName.SPARK,
                    openlineageAdapterVersion=Version("1.18.0"),
                ),
                spark_jobDetails=OpenLineageSparkJobDetailsRunFacet(
                    jobId=3,
                    jobDescription="Hive -> Clickhouse",
                ),
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydatabase.db/source_table",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="hdfs://test-hadoop:9820",
                        uri="hdfs://test-hadoop:9820",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(
                                name="dt",
                                type="timestamp",
                                description="Business date",
                            ),
                            OpenLineageSchemaField(
                                name="customer_id",
                                type="decimal(20,0)",
                            ),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                        ],
                    ),
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="hive://test-hadoop:9083",
                                name="mydatabase.source_table",
                                type=OpenLineageSymlinkType.TABLE,
                            ),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="clickhouse://localhost:8123",
                name="mydb.myschema.mytable",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="clickhouse://localhost:8123",
                        uri="clickhouse://localhost:8123",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="dt", type="timestamp"),
                            OpenLineageSchemaField(
                                name="customer_id",
                                type="decimal(20,0)",
                            ),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                        ],
                    ),
                    lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.OVERWRITE,
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "dt": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="hdfs://test-hadoop:9820",
                                        name="/user/hive/warehouse/mydatabase.db/source_table",
                                        field="dt",
                                        transformations=[
                                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                                type="DIRECT",
                                                subtype="IDENTITY",
                                                description="",
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                            "customer_id": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="hdfs://test-hadoop:9820",
                                        name="/user/hive/warehouse/mydatabase.db/source_table",
                                        field="customer_id",
                                        transformations=[
                                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                                type="DIRECT",
                                                subtype="IDENTITY",
                                                description="",
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                            "total_spent": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="hdfs://test-hadoop:9820",
                                        name="/user/hive/warehouse/mydatabase.db/source_table",
                                        field="total_spent",
                                        transformations=[
                                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                                type="DIRECT",
                                                subtype="IDENTITY",
                                                description="",
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                        },
                        dataset=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hdfs://test-hadoop:9820",
                                name="/user/hive/warehouse/mydatabase.db/source_table",
                                field="customer_id",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="JOIN",
                                        masking=False,
                                        description="ON (DISCOUNTS.CUSTOMERS_ID=CUSTOMERS.ID)",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                outputFacets=OpenLineageOutputDatasetFacets(
                    outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                        rowCount=10_000,
                        size=5_000_000,
                    ),
                ),
            ),
        ],
    )
