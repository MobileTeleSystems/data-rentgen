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
    SQLQueryDTO,
)


@pytest.fixture
def extracted_dbt_location() -> LocationDTO:
    return LocationDTO(
        type="local",
        name="somehost",
        addresses={"local://somehost"},
    )


@pytest.fixture
def extracted_spark_thrift_location() -> LocationDTO:
    return LocationDTO(
        type="spark",
        name="localhost:10000",
        addresses={"spark://localhost:10000"},
    )


@pytest.fixture
def extracted_dbt_job(
    extracted_dbt_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="dbt-run-demo_project",
        location=extracted_dbt_location,
        type=JobTypeDTO(type="DBT_JOB"),
    )


@pytest.fixture
def extracted_dbt_run(
    extracted_dbt_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("0196eccd-8aa4-7274-9116-824575596aaf"),
        job=extracted_dbt_job,
        user=None,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2025, 5, 20, 8, 26, 55, 524789, tzinfo=timezone.utc),
        ended_at=datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc),
        external_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
    )


@pytest.fixture
def extracted_dbt_sql_query() -> SQLQueryDTO:
    return SQLQueryDTO(
        query="select\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
    )


@pytest.fixture
def extracted_dbt_operation(
    extracted_dbt_run: RunDTO,
    extracted_dbt_sql_query: SQLQueryDTO,
):
    return OperationDTO(
        id=UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29"),
        name="demo_schema.demo_project.target_table",
        group="MODEL",
        run=extracted_dbt_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2025, 5, 20, 8, 27, 16, 601799, tzinfo=timezone.utc),
        ended_at=datetime(2025, 5, 20, 8, 27, 18, 581235, tzinfo=timezone.utc),
        sql_query=extracted_dbt_sql_query,
    )


@pytest.fixture
def extracted_dbt_spark_source_dataset(
    extracted_spark_thrift_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_spark_thrift_location,
        name="demo_schema.source_table",
    )


@pytest.fixture
def extracted_dbt_spark_source_schema() -> SchemaDTO:
    return SchemaDTO(
        fields=[
            {"name": "id", "description": "The primary key for this table"},
        ],
    )


@pytest.fixture
def extracted_dbt_spark_input(
    extracted_dbt_operation: OperationDTO,
    extracted_dbt_spark_source_dataset: DatasetDTO,
    extracted_dbt_spark_source_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        operation=extracted_dbt_operation,
        dataset=extracted_dbt_spark_source_dataset,
        schema=extracted_dbt_spark_source_schema,
    )


@pytest.fixture
def extracted_dbt_spark_target_dataset(
    extracted_spark_thrift_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_spark_thrift_location,
        name="demo_schema.target_table",
    )


@pytest.fixture
def extracted_dbt_spark_output(
    extracted_dbt_operation: OperationDTO,
    extracted_dbt_spark_target_dataset: DatasetDTO,
) -> OutputDTO:
    return OutputDTO(
        type=OutputTypeDTO.APPEND,
        operation=extracted_dbt_operation,
        dataset=extracted_dbt_spark_target_dataset,
        num_rows=2,
    )
