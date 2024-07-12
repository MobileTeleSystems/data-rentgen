from datetime import datetime, timezone

import pytest
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


def test_extractors_extract_run_airflow_task():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(namespace="airflow://airflow-host:8081", name="mydag.mytask"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__123",
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
                type="airflow",
                name="airflow-host:8081",
                addresses=["airflow://airflow-host:8081"],
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        external_id="manual__123",
        attempt="1",
        persistent_log_url=None,
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
