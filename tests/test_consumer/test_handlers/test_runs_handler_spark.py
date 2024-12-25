import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from uuid6 import UUID

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    DatasetSymlinkType,
    Input,
    Job,
    JobType,
    Location,
    Operation,
    OperationStatus,
    OperationType,
    Output,
    OutputType,
    Run,
    RunStatus,
    Schema,
)

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_spark() -> list[dict]:
    lines = (RESOURCES_PATH / "events_spark.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.asyncio
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
async def test_runs_handler_spark(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_spark: list[dict],
    input_transformation,
):
    for event in input_transformation(events_spark):
        await test_broker.publish(event, "input.runs")

    job_query = select(Job).order_by(Job.name).options(selectinload(Job.location).selectinload(Location.addresses))
    job_scalars = await async_session.scalars(job_query)
    jobs = job_scalars.all()
    assert len(jobs) == 1
    assert jobs[0].name == "spark_session"
    assert jobs[0].location.type == "local"
    assert jobs[0].location.name == "some.host.name"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "local://some.host.name"
    assert jobs[0].type == JobType.SPARK_APPLICATION

    run_query = select(Run).order_by(Run.id).options(selectinload(Run.started_by_user))
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    application_run = runs[0]
    assert application_run.id == UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    assert application_run.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert application_run.job_id == jobs[0].id
    assert application_run.status == RunStatus.SUCCEEDED
    assert application_run.started_at == datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc)
    assert application_run.started_by_user is not None
    assert application_run.started_by_user.name == "myuser"
    assert application_run.start_reason is None
    assert application_run.ended_at == datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc)
    assert application_run.external_id == "local-1719136537510"
    assert application_run.running_log_url == "http://127.0.0.1:4040"
    assert application_run.persistent_log_url is None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    job_operation = operations[0]
    assert job_operation.id == UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    assert job_operation.created_at == datetime(2024, 7, 5, 9, 6, 29, 463000, tzinfo=timezone.utc)
    assert job_operation.run_id == application_run.id
    assert job_operation.name == "execute_save_into_data_source_command"
    assert job_operation.type == OperationType.BATCH
    assert job_operation.status == OperationStatus.SUCCEEDED
    assert job_operation.started_at == datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    assert job_operation.ended_at == datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    assert job_operation.position == 3
    assert job_operation.description == "Hive -> Clickhouse"

    dataset_query = (
        select(Dataset)
        .order_by(Dataset.name)
        .options(
            selectinload(Dataset.location).selectinload(Location.addresses),
        )
    )
    dataset_scalars = await async_session.scalars(dataset_query)
    datasets = dataset_scalars.all()
    assert len(datasets) == 3

    hdfs_warehouse = datasets[0]
    hive_table = datasets[1]
    clickhouse_table = datasets[2]

    assert clickhouse_table.name == "mydb.myschema.mytable"
    assert clickhouse_table.location.type == "clickhouse"
    assert clickhouse_table.location.name == "localhost:8123"
    assert len(clickhouse_table.location.addresses) == 1
    assert clickhouse_table.location.addresses[0].url == "clickhouse://localhost:8123"
    assert clickhouse_table.format is None

    assert hive_table.name == "mydatabase.source_table"
    assert hive_table.location.type == "hive"
    assert hive_table.location.name == "test-hadoop:9083"
    assert len(hive_table.location.addresses) == 1
    assert hive_table.location.addresses[0].url == "hive://test-hadoop:9083"
    assert hive_table.format is None

    assert hdfs_warehouse.name == "/user/hive/warehouse/mydatabase.db/source_table"
    assert hdfs_warehouse.location.type == "hdfs"
    assert hdfs_warehouse.location.name == "test-hadoop:9820"
    assert len(hdfs_warehouse.location.addresses) == 1
    assert hdfs_warehouse.location.addresses[0].url == "hdfs://test-hadoop:9820"
    assert hdfs_warehouse.format is None

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert len(dataset_symlinks) == 2

    assert dataset_symlinks[0].from_dataset_id == hdfs_warehouse.id
    assert dataset_symlinks[0].to_dataset_id == hive_table.id
    assert dataset_symlinks[0].type == DatasetSymlinkType.METASTORE

    assert dataset_symlinks[1].from_dataset_id == hive_table.id
    assert dataset_symlinks[1].to_dataset_id == hdfs_warehouse.id
    assert dataset_symlinks[1].type == DatasetSymlinkType.WAREHOUSE

    schema_query = select(Schema).order_by(Schema.id)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert len(schemas) == 2

    hive_schema = schemas[0]
    assert hive_schema.fields == [
        {"name": "dt", "type": "timestamp", "description": "Business date"},
        {"name": "customer_id", "type": "decimal(20,0)"},
        {"name": "total_spent", "type": "float"},
    ]

    clickhouse_schema = schemas[1]
    assert clickhouse_schema.fields == [
        {"name": "dt", "type": "timestamp"},
        {"name": "customer_id", "type": "decimal(20,0)"},
        {"name": "total_spent", "type": "float"},
    ]

    input_query = select(Input).order_by(Input.dataset_id)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    hive_input = inputs[0]
    assert hive_input.created_at == datetime(2024, 7, 5, 9, 6, 29, 463000, tzinfo=timezone.utc)
    assert hive_input.operation_id == job_operation.id
    assert hive_input.run_id == application_run.id
    assert hive_input.job_id == application_run.job_id
    assert hive_input.dataset_id == hive_table.id
    assert hive_input.schema_id == hive_schema.id
    assert hive_input.num_bytes is None
    assert hive_input.num_rows is None
    assert hive_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    clickhouse_output = outputs[0]
    assert clickhouse_output.created_at == datetime(2024, 7, 5, 9, 6, 29, 463000, tzinfo=timezone.utc)
    assert clickhouse_output.operation_id == job_operation.id
    assert clickhouse_output.run_id == application_run.id
    assert clickhouse_output.job_id == application_run.job_id
    assert clickhouse_output.dataset_id == clickhouse_table.id
    assert clickhouse_output.type == OutputType.OVERWRITE
    assert clickhouse_output.schema_id == clickhouse_schema.id
    assert clickhouse_output.num_bytes == 5_000_000
    assert clickhouse_output.num_rows == 10_000
    assert clickhouse_output.num_files is None
