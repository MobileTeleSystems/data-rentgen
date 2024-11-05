from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.batch import extract_batch
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobIntegrationType,
    OpenLineageJobType,
    OpenLineageJobTypeJobFacet,
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
    OpenLineageProcessingEngineName,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
)


@pytest.fixture
def airflow_dag_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def airflow_dag_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
            ),
        ),
    )


@pytest.fixture
def airflow_task_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-7fc0-9d69-b1df9dac2c60")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
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


@pytest.fixture
def airflow_task_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-7fc0-9d69-b1df9dac2c60")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name=OpenLineageProcessingEngineName.AIRFLOW,
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
            ),
        ),
    )


@pytest.fixture
def extracted_airflow_location() -> LocationDTO:
    return LocationDTO(
        type="http",
        name="airflow-host:8081",
        addresses={"http://airflow-host:8081"},
    )


@pytest.fixture
def extracted_airflow_dag_job(
    extracted_airflow_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mydag",
        location=extracted_airflow_location,
        type=JobTypeDTO.AIRFLOW_DAG,
    )


@pytest.fixture
def extracted_airflow_task_job(
    extracted_airflow_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mydag.mytask",
        location=extracted_airflow_location,
        type=JobTypeDTO.AIRFLOW_TASK,
    )


@pytest.fixture
def extracted_airflow_dag_run(
    extracted_airflow_dag_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
        job=extracted_airflow_dag_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        start_reason=RunStartReasonDTO.MANUAL,
        ended_at=datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc),
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        persistent_log_url="http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00",
    )


@pytest.fixture
def extracted_airflow_task_run(
    extracted_airflow_task_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
        job=extracted_airflow_task_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        start_reason=RunStartReasonDTO.MANUAL,
        ended_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
        ),
    )


@pytest.mark.parametrize(
    "input_transformation",
    [
        # receiving data out of order does not change result
        pytest.param(
            list,
            id="preserve order",
        ),
        pytest.param(
            reversed,
            id="reverse order",
        ),
    ],
)
def test_extractors_extract_batch_airflow(
    airflow_dag_run_event_start: OpenLineageRunEvent,
    airflow_dag_run_event_stop: OpenLineageRunEvent,
    airflow_task_run_event_start: OpenLineageRunEvent,
    airflow_task_run_event_stop: OpenLineageRunEvent,
    extracted_airflow_location: LocationDTO,
    extracted_airflow_dag_job: JobDTO,
    extracted_airflow_task_job: JobDTO,
    extracted_airflow_dag_run: RunDTO,
    extracted_airflow_task_run: RunDTO,
    input_transformation,
):
    events = [
        airflow_dag_run_event_start,
        airflow_task_run_event_start,
        airflow_task_run_event_stop,
        airflow_dag_run_event_stop,
    ]

    extracted = extract_batch(input_transformation(events))

    assert extracted.locations() == [extracted_airflow_location]
    assert extracted.jobs() == [extracted_airflow_dag_job, extracted_airflow_task_job]
    assert extracted.runs() == [
        extracted_airflow_dag_run,
        extracted_airflow_task_run,
    ]

    assert not extracted.datasets()
    assert not extracted.dataset_symlinks()
    assert not extracted.users()
    assert not extracted.schemas()
    assert not extracted.operations()
    assert not extracted.inputs()
    assert not extracted.outputs()
