from datetime import datetime, timezone
from uuid import UUID

import pytest

from data_rentgen.dto import (
    DatasetDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    OutputDTO,
    OutputTypeDTO,
    RunDTO,
    RunStatusDTO,
    SchemaDTO,
)
from data_rentgen.dto.job_type import JobTypeDTO


@pytest.fixture
def extracted_flink_location() -> LocationDTO:
    return LocationDTO(
        type="http",
        name="flink-host:18081",
        addresses={"http://flink-host:18081"},
    )


@pytest.fixture
def extracted_flink_job(
    extracted_flink_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="myjob",
        location=extracted_flink_location,
        type=JobTypeDTO(type="FLINK_JOB"),
    )


@pytest.fixture
def extracted_flink_job_run(
    extracted_flink_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
        job=extracted_flink_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        start_reason=None,
        user=None,
        ended_at=datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc),
        external_id=None,
        attempt=None,
        persistent_log_url=None,
    )


@pytest.fixture
def extracted_flink_job_operation(
    extracted_flink_job_run: RunDTO,
) -> OperationDTO:
    return OperationDTO(
        id=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
        name="myjob",
        description=None,
        run=extracted_flink_job_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_flink_postgres_input(
    extracted_flink_job_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        operation=extracted_flink_job_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_flink_kafka_output(
    extracted_flink_job_operation: OperationDTO,
    extracted_kafka_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        type=OutputTypeDTO.APPEND,
        operation=extracted_flink_job_operation,
        dataset=extracted_kafka_dataset,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
    )
