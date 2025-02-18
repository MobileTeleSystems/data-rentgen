from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
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
    UserDTO,
)


@pytest.fixture
def extracted_postgres_location() -> LocationDTO:
    return LocationDTO(
        type="postgres",
        name="192.168.1.1:5432",
        addresses={"postgres://192.168.1.1:5432"},
    )


@pytest.fixture
def extracted_hdfs_location() -> LocationDTO:
    return LocationDTO(
        type="hdfs",
        name="test-hadoop:9820",
        addresses={"hdfs://test-hadoop:9820"},
    )


@pytest.fixture
def extracted_hive_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:9083",
        addresses={"hive://test-hadoop:9083"},
    )


@pytest.fixture
def extracted_spark_location() -> LocationDTO:
    return LocationDTO(
        type="local",
        name="some.host.com",
        addresses={"local://some.host.com"},
    )


@pytest.fixture
def extracted_postgres_dataset(
    extracted_postgres_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_postgres_location,
        name="mydb.myschema.mytable",
    )


@pytest.fixture
def extracted_hdfs_dataset(
    extracted_hdfs_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hdfs_location,
        name="/user/hive/warehouse/mydb.db/mytable",
    )


@pytest.fixture
def extracted_hive_dataset(
    extracted_hive_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hive_location,
        name="mydb.mytable",
    )


@pytest.fixture
def extracted_hdfs_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hdfs_dataset,
        to_dataset=extracted_hive_dataset,
        type=DatasetSymlinkTypeDTO.METASTORE,
    )


@pytest.fixture
def extracted_hive_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hive_dataset,
        to_dataset=extracted_hdfs_dataset,
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    )


@pytest.fixture
def extracted_dataset_schema() -> SchemaDTO:
    return SchemaDTO(
        fields=[
            {
                "name": "dt",
                "type": "timestamp",
                "description": "Business date",
            },
            {
                "name": "customer_id",
                "type": "decimal(20,0)",
            },
            {
                "name": "total_spent",
                "type": "float",
            },
            {
                "name": "phones",
                "type": "array",
                "fields": [
                    {
                        "name": "_element",
                        "type": "string",
                    },
                ],
            },
            {
                "name": "address",
                "type": "struct",
                "fields": [
                    {
                        "name": "street",
                        "type": "string",
                    },
                    {
                        "name": "city",
                        "type": "string",
                    },
                    {
                        "name": "state",
                        "type": "string",
                    },
                    {
                        "name": "zip",
                        "type": "string",
                    },
                ],
            },
        ],
    )


@pytest.fixture
def extracted_user() -> UserDTO:
    return UserDTO(name="myuser")


@pytest.fixture
def extracted_spark_app_job(
    extracted_spark_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession",
        location=extracted_spark_location,
        type=JobTypeDTO.SPARK_APPLICATION,
    )


@pytest.fixture
def extracted_spark_app_run(
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        job=extracted_spark_app_job,
        user=extracted_user,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
        external_id="local-1719136537510",
        running_log_url="http://127.0.0.1:4040",
    )


@pytest.fixture
def extracted_spark_operation(
    extracted_spark_app_run: RunDTO,
):
    return OperationDTO(
        id=UUID("01908225-1fd7-746b-910c-70d24f2898b1"),
        name="execute_some_command",
        run=extracted_spark_app_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_postgres_input(
    extracted_spark_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        operation=extracted_spark_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_hive_output(
    extracted_spark_operation: OperationDTO,
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        type=OutputTypeDTO.CREATE,
        operation=extracted_spark_operation,
        dataset=extracted_hive_dataset,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
    )


def get_operation_dto(operation_id: UUID) -> OperationDTO:
    return OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="local",
                    name="some.host.com",
                    addresses={
                        "local://some.host.com",
                    },
                    id=None,
                ),
                type=None,
                id=None,
            ),
            parent_run=None,
            status=RunStatusDTO.UNKNOWN,
            started_at=None,
            start_reason=None,
            user=None,
            ended_at=None,
            external_id=None,
            attempt=None,
            running_log_url=None,
            persistent_log_url=None,
        ),
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        group=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=None,
        ended_at=None,
    )
