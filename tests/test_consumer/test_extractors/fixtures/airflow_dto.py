from datetime import datetime, timezone
from uuid import UUID

import pytest

from data_rentgen.dto import (
    JobDTO,
    LocationDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
    UserDTO,
)
from data_rentgen.dto.job_type import JobTypeDTO


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
        type=JobTypeDTO(type="AIRFLOW_DAG"),
    )


@pytest.fixture
def extracted_airflow_task_job(
    extracted_airflow_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mydag.mytask",
        location=extracted_airflow_location,
        type=JobTypeDTO(type="AIRFLOW_TASK"),
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
        user=UserDTO(
            name="myuser",
            id=None,
        ),
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
        user=UserDTO(
            name="myuser",
            id=None,
        ),
        ended_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
        ),
    )
