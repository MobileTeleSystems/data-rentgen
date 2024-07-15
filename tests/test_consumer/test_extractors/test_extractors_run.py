from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_run
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run import OpenLineageRun
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowRunFacet,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageProcessingEngineName,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.dto import JobDTO, LocationDTO, RunDTO, RunStatusDTO


def test_extractors_extract_run_spark_app_yarn():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(namespace="yarn://cluster", name="myjob"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="yarn",
                    appName="myapp",
                    applicationId="application_1234_5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040",
                    proxyUrl="http://yarn-proxy:8088/proxy/application_1234_5678",
                    historyUrl="http://history-server:18081/history/application_1234_5678/",
                ),
            ),
        ),
    )
    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="yarn", name="cluster", addresses=["yarn://cluster"]),
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
        external_id="application_1234_5678",
        attempt=None,
        persistent_log_url="http://history-server:18081/history/application_1234_5678/",
        running_log_url="http://yarn-proxy:8088/proxy/application_1234_5678",
    )


def test_extractors_extract_run_spark_app_local():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(namespace="host://some.host.com", name="myjob"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[4]",
                    appName="myapp",
                    applicationId="local-1234-5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040",
                ),
            ),
        ),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="host", name="some.host.com", addresses=["host://some.host.com"]),
        ),
        status=RunStatusDTO.STARTED,
        started_at=None,
        external_id="local-1234-5678",
        attempt=None,
        persistent_log_url=None,
        running_log_url="http://localhost:4040",
    )


def test_extractors_extract_run_airflow_task_with_ti_log_url():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(namespace="http://airflow-host:8081", name="mydag.mytask"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.9.0"),
                ),
                airflow=OpenLineageAirflowRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        log_url=(
                            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
                        ),
                    ),
                ),
            ),
        ),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses=["http://airflow-host:8081"],
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_2_9_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(namespace="http://airflow-host:8081", name="mydag.mytask"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.9.0"),
                ),
                airflow=OpenLineageAirflowRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses=["http://airflow-host:8081"],
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_2_x():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(namespace="http://airflow-host:8081", name="mydag.mytask"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses=["http://airflow-host:8081"],
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.FAIL, RunStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, RunStatusDTO.KILLED),
        (OpenLineageRunEventType.OTHER, None),
    ],
)
def test_extractors_extract_run_unknown(event_type: OpenLineageRunEventType, expected_status: RunStatusDTO | None):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(namespace="something", name="myjob"),
        run=OpenLineageRun(runId=run_id),
    )

    ended_at = now if expected_status else None
    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses=["unknown://something"],
            ),
        ),
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
