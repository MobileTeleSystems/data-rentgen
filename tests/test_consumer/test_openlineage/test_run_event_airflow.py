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
    OpenLineageDatasetFacets,
    OpenLineageDatasourceDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageDocumentationJobFacet,
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
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


def test_run_event_airflow_dag_start():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:04:13.979349+00:00",
        "eventType": "START",
        "job": {
            "name": "mydag",
            "namespace": "http://airflow-host:8081",
            "facets": {
                "airflow": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "taskGroups": {},
                    "taskTree": {
                        "mytask": {},
                    },
                    "tasks": {
                        "mytask": {
                            "emits_ol_events": True,
                            "is_setup": False,
                            "is_teardown": False,
                            "operator": "SQLExecuteQueryOperator",
                            "ui_color": "#fff",
                            "ui_fgcolor": "#000",
                            "ui_label": "mytask",
                        },
                    },
                },
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "DAG",
                    "processingType": "BATCH",
                },
                "ownership": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json#/$defs/OwnershipJobFacet",
                    "owners": [{"name": "airflow"}],
                },
            },
        },
        "run": {
            "runId": "01908223-0782-79b8-9495-b1c38aaee839",
            "facets": {
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json#/$defs/NominalTimeRunFacet",
                    "nominalEndTime": "2024-07-05T09:04:12.162809+00:00",
                    "nominalStartTime": "2024-07-05T09:04:12.162809+00:00",
                },
                "airflowDagRun": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "dag": {
                        "dag_id": "mydag",
                        "owner": "myuser",
                        "schedule_interval": "@once",
                        "tags": [
                            "some",
                            "tag",
                        ],
                        "timetable": {},
                    },
                    "dagRun": {
                        "conf": {},
                        "dag_id": "mydag",
                        "data_interval_end": "2024-07-05T09:04:12.162809+00:00",
                        "data_interval_start": "2024-07-05T09:04:12.162809+00:00",
                        "external_trigger": True,
                        "run_id": "manual__2024-07-05T09:04:12.162809+00:00",
                        "run_type": "manual",
                        "start_date": "2024-07-05T09:04:13.979349+00:00",
                    },
                },
            },
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
            facets=OpenLineageRunFacets(
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(
                        dag_id="mydag",
                        owner="myuser",
                    ),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:12.162809+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc),
                    ),
                ),
            ),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_dag_end():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:08:05.691973+00:00",
        "eventType": "COMPLETE",
        "job": {
            "name": "mydag",
            "namespace": "http://airflow-host:8081",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "DAG",
                    "processingType": "BATCH",
                },
            },
        },
        "run": {
            "runId": "01908223-0782-79b8-9495-b1c38aaee839",
            "facets": {
                "airflowState": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "dagRunState": "success",
                    "tasksState": {"mytask": "success"},
                },
            },
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_task_start():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:04:20.783845+00:00",
        "eventType": "START",
        "job": {
            "name": "mydag.mytask",
            "namespace": "http://airflow-host:8081",
            "facets": {
                "documentation": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DocumentationJobFacet.json#/$defs/DocumentationJobFacet",
                    "description": "Task description",
                },
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "TASK",
                    "processingType": "BATCH",
                },
                "ownership": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json#/$defs/OwnershipJobFacet",
                    "owners": [{"name": "myuser"}],
                },
                "sql": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                    "query": (
                        "\n    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)"
                        "\n    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,"
                        "\n           order_placed_on,"
                        "\n           COUNT(*) AS orders_placed"
                        "\n      FROM top_delivery_times"
                        "\n     GROUP BY order_placed_on"
                        "\n    "
                    ),
                },
            },
        },
        "run": {
            "runId": "01908223-0782-7fc0-9d69-b1df9dac2c60",
            "facets": {
                "airflow": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "dag": {
                        "dag_id": "mydag",
                        "owner": "myuser",
                        "schedule_interval": "@once",
                        "tags": [
                            "some",
                            "tag",
                        ],
                        "timetable": {},
                    },
                    "dagRun": {
                        "conf": {},
                        "dag_id": "mydag",
                        "data_interval_end": "2024-07-05T09:04:12.162809+00:00",
                        "data_interval_start": "2024-07-05T09:04:12.162809+00:00",
                        "external_trigger": True,
                        "run_id": "manual__2024-07-05T09:04:12.162809+00:00",
                        "run_type": "manual",
                        "start_date": "2024-07-05T09:04:13.979349+00:00",
                    },
                    "task": {
                        "depends_on_past": False,
                        "downstream_task_ids": "[]",
                        "executor_config": {},
                        "ignore_first_depends_on_past": True,
                        "inlets": [],
                        "is_setup": False,
                        "is_teardown": False,
                        "mapped": False,
                        "multiple_outputs": False,
                        "operator_class": "SQLExecuteQueryOperator",
                        "outlets": [],
                        "owner": "myuser",
                        "priority_weight": 1,
                        "queue": "default",
                        "retries": 0,
                        "retry_exponential_backoff": False,
                        "task_id": "mytask",
                        "trigger_rule": "all_success",
                        "upstream_task_ids": "[]",
                        "wait_for_downstream": False,
                        "wait_for_past_depends_before_skipping": False,
                        "weight_rule": "<<non-serializable: _DownstreamPriorityWeightStrategy>>",
                    },
                    "taskInstance": {
                        "pool": "default_pool",
                        "queued_dttm": "2024-07-05T09:04:12.162809+00:00",
                        "try_number": 1,
                        "log_url": "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask",
                    },
                    "taskUuid": "01908223-0782-7fc0-9d69-b1df9dac2c60",
                },
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/NominalTimeRunFacet.json#/$defs/NominalTimeRunFacet",
                    "nominalEndTime": "2024-07-05T09:04:12.162809+00:00",
                    "nominalStartTime": "2024-07-05T09:04:12.162809+00:00",
                },
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "http://airflow-host:8081"},
                    "run": {"runId": "01908223-0782-79b8-9495-b1c38aaee839"},
                },
                "processing_engine": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json#/$defs/ProcessingEngineRunFacet",
                    "name": "Airflow",
                    "openlineageAdapterVersion": "2.2.0",
                    "version": "2.10.5",
                },
                "unknownSourceAttribute": {
                    "_producer": "https://github.com/apache/myuser/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "unknownItems": [
                        {
                            "name": "SQLExecuteQueryOperator",
                            "properties": {
                                "depends_on_past": False,
                                "downstream_task_ids": "[]",
                                "executor_config": {},
                                "ignore_first_depends_on_past": True,
                                "inlets": [],
                                "is_setup": False,
                                "is_teardown": False,
                                "mapped": False,
                                "multiple_outputs": False,
                                "operator_class": "SQLExecuteQueryOperator",
                                "outlets": [],
                                "owner": "myuser",
                                "priority_weight": 1,
                                "queue": "default",
                                "retries": 0,
                                "retry_exponential_backoff": False,
                                "task_id": "mytask",
                                "trigger_rule": "all_success",
                                "upstream_task_ids": "[]",
                                "wait_for_downstream": False,
                                "wait_for_past_depends_before_skipping": False,
                                "weight_rule": "<<non-serializable: _DownstreamPriorityWeightStrategy>>",
                            },
                            "type": "operator",
                        },
                    ],
                },
            },
        },
        "inputs": [
            {
                "namespace": "postgres://postgres:5432",
                "name": "food_delivery.public.top_delivery_times",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "postgres://postgres:5432",
                        "uri": "postgres://postgres:5432/food_delivery",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "order_id",
                                "type": "integer",
                            },
                            {
                                "name": "order_placed_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "order_dispatched_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "order_delivery_time",
                                "type": "double precision",
                            },
                        ],
                    },
                },
                "inputFacets": {},
            },
        ],
        "outputs": [
            {
                "name": "food_delivery.public.popular_orders_day_of_week",
                "namespace": "postgres://postgres:5432",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "postgres://postgres:5432",
                        "uri": "postgres://postgres:5432/food_delivery",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "order_day_of_week",
                                "type": "varchar",
                            },
                            {
                                "name": "order_placed_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "orders_placed",
                                "type": "int4",
                            },
                        ],
                    },
                    "columnLineage": {
                        "fields": {
                            "order_day_of_week": {
                                "inputFields": [
                                    {
                                        "field": "order_placed_on",
                                        "name": "food_delivery.public.top_delivery_times",
                                        "namespace": "postgres://postgres:5432",
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                            "order_placed_on": {
                                "inputFields": [
                                    {
                                        "field": "order_placed_on",
                                        "name": "food_delivery.public.top_delivery_times",
                                        "namespace": "postgres://postgres:5432",
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                        },
                    },
                },
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 4, 20, 783845, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
                documentation=OpenLineageDocumentationJobFacet(description="Task description"),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)"
                        "\n    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,"
                        "\n           order_placed_on,"
                        "\n           COUNT(*) AS orders_placed"
                        "\n      FROM top_delivery_times"
                        "\n     GROUP BY order_placed_on"
                        "\n    "
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="mydag",
                        namespace="http://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
                    ),
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.10.5"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.2.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(
                        dag_id="mydag",
                        owner="myuser",
                    ),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:12.162809+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc),
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        log_url=(
                            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
                        ),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="SQLExecuteQueryOperator",
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="postgres://postgres:5432",
                name="food_delivery.public.top_delivery_times",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="postgres://postgres:5432",
                        uri="postgres://postgres:5432/food_delivery",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="order_id", type="integer"),
                            OpenLineageSchemaField(name="order_placed_on", type="timestamp"),
                            OpenLineageSchemaField(name="order_dispatched_on", type="timestamp"),
                            OpenLineageSchemaField(name="order_delivery_time", type="double precision"),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="postgres://postgres:5432",
                name="food_delivery.public.popular_orders_day_of_week",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="postgres://postgres:5432",
                        uri="postgres://postgres:5432/food_delivery",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="order_day_of_week", type="varchar"),
                            OpenLineageSchemaField(name="order_placed_on", type="timestamp"),
                            OpenLineageSchemaField(name="orders_placed", type="int4"),
                        ],
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "order_day_of_week": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="postgres://postgres:5432",
                                        name="food_delivery.public.top_delivery_times",
                                        field="order_placed_on",
                                    ),
                                ],
                                transformationDescription="",
                                transformationType="",
                            ),
                            "order_placed_on": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="postgres://postgres:5432",
                                        name="food_delivery.public.top_delivery_times",
                                        field="order_placed_on",
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


def test_run_event_airflow_task_complete():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:07:37.858423+00:00",
        "eventType": "COMPLETE",
        "job": {
            "name": "mydag.mytask",
            "namespace": "http://airflow-host:8081",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "TASK",
                    "processingType": "BATCH",
                },
                "sql": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
                    "query": (
                        "\n    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)"
                        "\n    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,"
                        "\n           order_placed_on,"
                        "\n           COUNT(*) AS orders_placed"
                        "\n      FROM top_delivery_times"
                        "\n     GROUP BY order_placed_on"
                        "\n    "
                    ),
                },
            },
        },
        "run": {
            "runId": "01908223-0782-7fc0-9d69-b1df9dac2c60",
            "facets": {
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "http://airflow-host:8081"},
                    "run": {"runId": "01908223-0782-79b8-9495-b1c38aaee839"},
                },
                "unknownSourceAttribute": {
                    "_producer": "https://github.com/apache/myuser/tree/providers-openlineage/2.2.0",
                    "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet",
                    "unknownItems": [
                        {
                            "name": "SQLExecuteQueryOperator",
                            "properties": {
                                "depends_on_past": False,
                                "downstream_task_ids": "[]",
                                "executor_config": {},
                                "ignore_first_depends_on_past": True,
                                "inlets": [],
                                "is_setup": False,
                                "is_teardown": False,
                                "mapped": False,
                                "multiple_outputs": False,
                                "operator_class": "SQLExecuteQueryOperator",
                                "outlets": [],
                                "owner": "myuser",
                                "priority_weight": 1,
                                "queue": "default",
                                "retries": 0,
                                "retry_exponential_backoff": False,
                                "task_id": "mytask",
                                "trigger_rule": "all_success",
                                "upstream_task_ids": "[]",
                                "wait_for_downstream": False,
                                "wait_for_past_depends_before_skipping": False,
                                "weight_rule": "<<non-serializable: _DownstreamPriorityWeightStrategy>>",
                            },
                            "type": "operator",
                        },
                    ],
                },
            },
        },
        "inputs": [
            {
                "namespace": "postgres://postgres:5432",
                "name": "food_delivery.public.top_delivery_times",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "postgres://postgres:5432",
                        "uri": "postgres://postgres:5432/food_delivery",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "order_id",
                                "type": "integer",
                            },
                            {
                                "name": "order_placed_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "order_dispatched_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "order_delivery_time",
                                "type": "double precision",
                            },
                        ],
                    },
                },
                "inputFacets": {},
            },
        ],
        "outputs": [
            {
                "name": "food_delivery.public.popular_orders_day_of_week",
                "namespace": "postgres://postgres:5432",
                "facets": {
                    "dataSource": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                        "name": "postgres://postgres:5432",
                        "uri": "postgres://postgres:5432/food_delivery",
                    },
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.32.0/integration/airflow",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                        "fields": [
                            {
                                "name": "order_day_of_week",
                                "type": "varchar",
                            },
                            {
                                "name": "order_placed_on",
                                "type": "timestamp",
                            },
                            {
                                "name": "orders_placed",
                                "type": "int4",
                            },
                        ],
                    },
                    "columnLineage": {
                        "fields": {
                            "order_day_of_week": {
                                "inputFields": [
                                    {
                                        "field": "order_placed_on",
                                        "name": "food_delivery.public.top_delivery_times",
                                        "namespace": "postgres://postgres:5432",
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                            "order_placed_on": {
                                "inputFields": [
                                    {
                                        "field": "order_placed_on",
                                        "name": "food_delivery.public.top_delivery_times",
                                        "namespace": "postgres://postgres:5432",
                                    },
                                ],
                                "transformationDescription": "",
                                "transformationType": "",
                            },
                        },
                    },
                },
            },
        ],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 7, 37, 858423, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)"
                        "\n    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,"
                        "\n           order_placed_on,"
                        "\n           COUNT(*) AS orders_placed"
                        "\n      FROM top_delivery_times"
                        "\n     GROUP BY order_placed_on"
                        "\n    "
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="mydag",
                        namespace="http://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
                    ),
                ),
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="postgres://postgres:5432",
                name="food_delivery.public.top_delivery_times",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="postgres://postgres:5432",
                        uri="postgres://postgres:5432/food_delivery",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="order_id", type="integer"),
                            OpenLineageSchemaField(name="order_placed_on", type="timestamp"),
                            OpenLineageSchemaField(name="order_dispatched_on", type="timestamp"),
                            OpenLineageSchemaField(name="order_delivery_time", type="double precision"),
                        ],
                    ),
                ),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="postgres://postgres:5432",
                name="food_delivery.public.popular_orders_day_of_week",
                facets=OpenLineageDatasetFacets(
                    dataSource=OpenLineageDatasourceDatasetFacet(
                        name="postgres://postgres:5432",
                        uri="postgres://postgres:5432/food_delivery",
                    ),
                    schema=OpenLineageSchemaDatasetFacet(
                        fields=[
                            OpenLineageSchemaField(name="order_day_of_week", type="varchar"),
                            OpenLineageSchemaField(name="order_placed_on", type="timestamp"),
                            OpenLineageSchemaField(name="orders_placed", type="int4"),
                        ],
                    ),
                    columnLineage=OpenLineageColumnLineageDatasetFacet(
                        fields={
                            "order_day_of_week": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="postgres://postgres:5432",
                                        name="food_delivery.public.top_delivery_times",
                                        field="order_placed_on",
                                    ),
                                ],
                                transformationDescription="",
                                transformationType="",
                            ),
                            "order_placed_on": OpenLineageColumnLineageDatasetFacetField(
                                inputFields=[
                                    OpenLineageColumnLineageDatasetFacetFieldRef(
                                        namespace="postgres://postgres:5432",
                                        name="food_delivery.public.top_delivery_times",
                                        field="order_placed_on",
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
