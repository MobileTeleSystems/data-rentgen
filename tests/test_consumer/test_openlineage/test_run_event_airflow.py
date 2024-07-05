from datetime import datetime, timezone

from packaging.version import Version
from pydantic import TypeAdapter
from uuid6 import UUID

from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacetsDict,
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
    OpenLineageRunFacetsDict,
)

RunEventAdapter = TypeAdapter(OpenLineageRunEvent)


def test_run_event_airflow_dag_start():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T07:54:16.527637+00:00",
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
            "runId": "01903c2f-49a3-717f-b421-2aee9401d70b",
            "facets": {
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalEndTime": "2024-06-21T19:04:10.403902+00:00",
                    "nominalStartTime": "2024-06-21T19:04:10.403902+00:00",
                },
            },
        },
        "inputs": [],
        "outputs": [],
    }

    assert RunEventAdapter.validate_python(json) == OpenLineageRunEvent(
        eventTime=datetime(
            year=2024,
            month=7,
            day=5,
            hour=7,
            minute=54,
            second=16,
            microsecond=527637,
            tzinfo=timezone.utc,
        ),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacetsDict(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
            # unknown facets are ignored
        ),
        run=OpenLineageRun(
            runId=UUID("01903c2f-49a3-717f-b421-2aee9401d70b"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_dag_end():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T07:55:59.256427+00:00",
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
            "runId": "01903c2f-49a3-717f-b421-2aee9401d70b",
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
        eventTime=datetime(
            year=2024,
            month=7,
            day=5,
            hour=7,
            minute=55,
            second=59,
            microsecond=256427,
            tzinfo=timezone.utc,
        ),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacetsDict(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
            # unknown facets are ignored
        ),
        run=OpenLineageRun(
            runId=UUID("01903c2f-49a3-717f-b421-2aee9401d70b"),
            # unknown facets are ignored
        ),
        inputs=[],
        outputs=[],
    )


def test_run_event_airflow_task_start():
    json = {
        "producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        "eventTime": "2024-07-05T07:54:18.951558+00:00",
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
            "runId": "01903c2f-49a3-7732-88e6-acaa71a6876f",
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
                        "data_interval_end": "2024-06-21T19:04:10.403902+00:00",
                        "data_interval_start": "2024-06-21T19:04:10.403902+00:00",
                        "external_trigger": True,
                        "run_id": "manual__2024-06-21T19:04:10.403902+00:00",
                        "run_type": "manual",
                        "start_date": "2024-07-05T07:54:16.527637+00:00",
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
                        "queued_dttm": "2024-07-05T07:54:16.676920+00:00",
                        "try_number": 1,
                    },
                    "taskUuid": "01903c2f-49a3-7732-88e6-acaa71a6876f",
                },
                "nominalTime": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalEndTime": "2024-06-21T19:04:10.403902+00:00",
                    "nominalStartTime": "2024-06-21T19:04:10.403902+00:00",
                },
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "airflow://airflow-host:8081"},
                    "run": {"runId": "01903c2f-49a3-717f-b421-2aee9401d70b"},
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
        eventTime=datetime(
            year=2024,
            month=7,
            day=5,
            hour=7,
            minute=54,
            second=18,
            microsecond=951558,
            tzinfo=timezone.utc,
        ),
        eventType=OpenLineageRunEventType.START,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacetsDict(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01903c2f-49a3-7732-88e6-acaa71a6876f"),
            facets=OpenLineageRunFacetsDict(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="mydag",
                        namespace="airflow://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01903c2f-49a3-717f-b421-2aee9401d70b"),
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
        "eventTime": "2024-07-05T07:55:51.098158+00:00",
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
            "runId": "01903c2f-49a3-7732-88e6-acaa71a6876f",
            "facets": {
                "parent": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "airflow://airflow-host:8081"},
                    "run": {"runId": "01903c2f-49a3-717f-b421-2aee9401d70b"},
                },
                "parentRun": {
                    "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.9.0",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
                    "job": {"name": "mydag", "namespace": "airflow://airflow-host:8081"},
                    "run": {"runId": "01903c2f-49a3-717f-b421-2aee9401d70b"},
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
        eventTime=datetime(
            year=2024,
            month=7,
            day=5,
            hour=7,
            minute=55,
            second=51,
            microsecond=98158,
            tzinfo=timezone.utc,
        ),
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacetsDict(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("01903c2f-49a3-7732-88e6-acaa71a6876f"),
            facets=OpenLineageRunFacetsDict(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="mydag",
                        namespace="airflow://airflow-host:8081",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01903c2f-49a3-717f-b421-2aee9401d70b"),
                    ),
                ),
            ),
        ),
        inputs=[],
        outputs=[],
    )
