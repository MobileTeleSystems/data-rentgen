from datetime import datetime, timezone
from uuid import UUID

import pytest

from data_rentgen.dto import (
    DatasetDTO,
    InputDTO,
    JobDTO,
    JobTypeDTO,
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


@pytest.fixture
def extracted_unknown_run_location() -> LocationDTO:
    return LocationDTO(
        type="local",
        name="some.host.com",
        addresses={"local://some.host.com"},
    )


@pytest.fixture
def extracted_unknown_job(
    extracted_unknown_run_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="somerun",
        location=extracted_unknown_run_location,
        type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
    )


@pytest.fixture
def extracted_unknown_run(
    extracted_unknown_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        job=extracted_unknown_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_unknown_operation(
    extracted_unknown_run: RunDTO,
):
    return OperationDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        name="somerun",
        run=extracted_unknown_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_unknown_postgres_input(
    extracted_unknown_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        operation=extracted_unknown_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_unknown_hdfs_output(
    extracted_unknown_operation: OperationDTO,
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        type=OutputTypeDTO.CREATE,
        operation=extracted_unknown_operation,
        dataset=extracted_hdfs_dataset1,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
    )
