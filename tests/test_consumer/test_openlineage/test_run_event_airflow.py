from datetime import datetime, timezone

from packaging.version import Version
from pydantic import TypeAdapter
from uuid6 import UUID

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
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


def test_run_event_airflow_dag_start():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:04:13.979349+00:00",
        "eventType": "START",
        "job": {
            "name": "mydag",
            "namespace": "airflow://airflow-host:8081",
            "facets": {
                "airflow": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                    "taskGroups": {},
                    "taskTree": {
                        "mytask": {},
                    },
                    "tasks": {
                        "mytask": {
                            "emits_ol_events": True,
                            "is_setup": False,
                            "is_teardown": False,
                            "operator": "SSHOperator",
                            "ui_color": "#fff",
                            "ui_fgcolor": "#000",
                            "ui_label": "mytask",
                        },
                    },
                },
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "DAG",
                    "processingType": "BATCH",
                },
                "ownership": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/OwnershipJobFacet",
                    "owners": [{"name": "airflow"}],
                },
            },
        },
        "run": {
            "runId": "01908223-0782-79b8-9495-b1c38aaee839",
            "facets": {
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalEndTime": "2024-07-05T09:04:12.162809+00:00",
                    "nominalStartTime": "2024-07-05T09:04:12.162809+00:00",
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
            namespace="airflow://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
            # unknown facets are ignored
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_dag_end():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:08:05.691973+00:00",
        "eventType": "COMPLETE",
        "job": {
            "name": "mydag",
            "namespace": "airflow://airflow-host:8081",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/JobTypeJobFacet",
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
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
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
            namespace="airflow://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
            # unknown facets are ignored
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
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:04:20.783845+00:00",
        "eventType": "START",
        "job": {
            "name": "mydag.mytask",
            "namespace": "airflow://airflow-host:8081",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "TASK",
                    "processingType": "BATCH",
                },
                "ownership": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/OwnershipJobFacet",
                    "owners": [{"name": "myuser"}],
                },
            },
        },
        "run": {
            "runId": "01908223-0782-7fc0-9d69-b1df9dac2c60",
            "facets": {
                "airflow": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
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
                        "operator_class": "SSHOperator",
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
                    },
                    "taskUuid": "01908223-0782-7fc0-9d69-b1df9dac2c60",
                },
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalEndTime": "2024-07-05T09:04:12.162809+00:00",
                    "nominalStartTime": "2024-07-05T09:04:12.162809+00:00",
                },
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "airflow://airflow-host:8081"},
                    "run": {"runId": "01908223-0782-79b8-9495-b1c38aaee839"},
                },
                "processing_engine": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ProcessingEngineRunFacet",
                    "name": "Airflow",
                    "openlineageAdapterVersion": "1.9.0",
                    "version": "2.9.2",
                },
                "unknownSourceAttribute": {
                    "_producer": "https://github.com/apache/myuser/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                    "unknownItems": [
                        {
                            "name": "SSHOperator",
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
                                "operator_class": "SSHOperator",
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
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 4, 20, 783845, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
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
                        namespace="airflow://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
                    ),
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.9.0"),
                ),
                # unknown facets are ignored
            ),
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_task_complete():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T09:07:37.858423+00:00",
        "eventType": "COMPLETE",
        "job": {
            "name": "mydag.mytask",
            "namespace": "airflow://airflow-host:8081",
            "facets": {
                "jobType": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/JobTypeJobFacet",
                    "integration": "AIRFLOW",
                    "jobType": "TASK",
                    "processingType": "BATCH",
                },
            },
        },
        "run": {
            "runId": "01908223-0782-7fc0-9d69-b1df9dac2c60",
            "facets": {
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "airflow://airflow-host:8081"},
                    "run": {"runId": "01908223-0782-79b8-9495-b1c38aaee839"},
                },
                "unknownSourceAttribute": {
                    "_producer": "https://github.com/apache/myuser/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                    "unknownItems": [
                        {
                            "name": "SSHOperator",
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
                                "operator_class": "SSHOperator",
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
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(2024, 7, 5, 9, 7, 37, 858423, tzinfo=timezone.utc),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="mydag",
                        namespace="airflow://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
                    ),
                ),
            ),
        ),
        inputs=[],
        outputs=[],
    )
