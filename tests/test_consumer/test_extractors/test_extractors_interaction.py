from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import (
    DatasetDTO,
    InputDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    OutputTypeDTO,
    SchemaDTO,
    SQLQueryDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageInputDatasetFacets,
    OpenLineageInputStatisticsInputDatasetFacet,
    OpenLineageLifecycleStateChangeDatasetFacet,
    OpenLineageOutputDatasetFacets,
    OpenLineageOutputStatisticsOutputDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.openlineage.run_event import OpenLineageRunEvent


@pytest.mark.parametrize("dataset_type", [OpenLineageInputDataset, OpenLineageOutputDataset])
def test_extractors_extract_input_output_schema(dataset_type: type[OpenLineageDataset]):
    dataset = dataset_type(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="",
                        description="",
                        fields=[],
                    ),
                    OpenLineageSchemaField(name="customer_id", type="decimal(20,0)", description=" some "),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(name="_element", type="string"),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(name="street", type="string"),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )

    assert GenericExtractor().extract_schema(dataset) == SchemaDTO(
        fields=[
            {"name": "dt"},  # all empty values are excluded
            {"name": "customer_id", "type": "decimal(20,0)", "description": "some"},  # spaces are trimmed
            {"name": "total_spent", "type": "float"},
            {
                "name": "phones",
                "type": "array",
                "fields": [{"name": "_element", "type": "string"}],
            },
            {
                "name": "address",
                "type": "struct",
                "fields": [
                    {"name": "street", "type": "string"},
                    {"name": "city", "type": "string"},
                    {"name": "state", "type": "string"},
                    {"name": "zip", "type": "string"},
                ],
            },
        ],
    )


@pytest.mark.parametrize("dataset_type", [OpenLineageInputDataset, OpenLineageOutputDataset])
def test_extractors_extract_input_output_schema_no_fields(dataset_type: type[OpenLineageDataset]):
    dataset = dataset_type(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            schema=OpenLineageSchemaDatasetFacet(
                fields=[],
            ),
        ),
    )

    assert GenericExtractor().extract_schema(dataset) is None


@pytest.mark.parametrize(
    ["row_count", "byte_count", "file_count"],
    [
        (1_000_000, 1000 * 1024 * 1024, 10),
        (0, 0, 0),
        (None, None, None),
    ],
)
def test_extractors_extract_input(
    row_count: int | None,
    byte_count: int | None,
    file_count: int | None,
):
    input = OpenLineageInputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        inputFacets=OpenLineageInputDatasetFacets(
            inputStatistics=OpenLineageInputStatisticsInputDatasetFacet(
                rowCount=row_count,
                size=byte_count,
                fileCount=file_count,
            ),
        ),
    )
    operation = Mock(spec=OperationDTO)
    event = Mock(spec=OpenLineageRunEvent)
    operation.created_at = event.eventTime = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)

    assert GenericExtractor().extract_input(operation, input, event) == (
        InputDTO(
            created_at=operation.created_at,
            operation=operation,
            dataset=DatasetDTO(
                name="/user/hive/warehouse/mydb.db/mytable",
                location=LocationDTO(
                    type="hdfs",
                    name="test-hadoop:9820",
                    addresses={"hdfs://test-hadoop:9820"},
                ),
            ),
            num_rows=row_count,
            num_bytes=byte_count,
            num_files=file_count,
        ),
        [],
    )


def test_extractors_extract_input_for_long_operations():
    input_ = OpenLineageInputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
    )

    # operation was created long time ago
    operation = Mock(spec=OperationDTO)
    operation.created_at = datetime(2024, 7, 5, tzinfo=timezone.utc)

    event = Mock(spec=OpenLineageRunEvent)
    event.eventTime = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)

    assert GenericExtractor().extract_input(operation, input_, event) == (
        InputDTO(
            # count only whole hours since operation was created
            created_at=operation.created_at + timedelta(hours=9),
            operation=operation,
            dataset=DatasetDTO(
                name="/user/hive/warehouse/mydb.db/mytable",
                location=LocationDTO(
                    type="hdfs",
                    name="test-hadoop:9820",
                    addresses={"hdfs://test-hadoop:9820"},
                ),
            ),
        ),
        [],
    )


@pytest.mark.parametrize(
    ["lifecycle_state_change", "expected_type"],
    [
        (OpenLineageDatasetLifecycleStateChange.CREATE, OutputTypeDTO.CREATE),
        (OpenLineageDatasetLifecycleStateChange.OVERWRITE, OutputTypeDTO.OVERWRITE),
        (OpenLineageDatasetLifecycleStateChange.ALTER, OutputTypeDTO.ALTER),
        (OpenLineageDatasetLifecycleStateChange.DROP, OutputTypeDTO.DROP),
        (OpenLineageDatasetLifecycleStateChange.RENAME, OutputTypeDTO.RENAME),
        (OpenLineageDatasetLifecycleStateChange.TRUNCATE, OutputTypeDTO.TRUNCATE),
    ],
)
@pytest.mark.parametrize(
    ["row_count", "byte_count", "file_count"],
    [
        (1_000_000, 1000 * 1024 * 1024, 10),
        (0, 0, 0),
        (None, None, None),
    ],
)
def test_extractors_extract_output_batch_with_lifecycle(
    lifecycle_state_change: OpenLineageDatasetLifecycleStateChange,
    expected_type: OutputTypeDTO,
    row_count: int | None,
    byte_count: int | None,
    file_count: int | None,
):
    output = OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                lifecycleStateChange=lifecycle_state_change,
            ),
        ),
        outputFacets=OpenLineageOutputDatasetFacets(
            outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                rowCount=row_count,
                size=byte_count,
                fileCount=file_count,
            ),
        ),
    )
    operation = Mock(spec=OperationDTO)
    event = Mock(spec=OpenLineageRunEvent)
    operation.created_at = event.eventTime = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)

    assert GenericExtractor().extract_output(operation, output, event) == (
        OutputDTO(
            created_at=operation.created_at,
            type=expected_type,
            operation=operation,
            dataset=DatasetDTO(
                name="/user/hive/warehouse/mydb.db/mytable",
                location=LocationDTO(
                    type="hdfs",
                    name="test-hadoop:9820",
                    addresses={"hdfs://test-hadoop:9820"},
                ),
            ),
            num_rows=row_count,
            num_bytes=byte_count,
            num_files=file_count,
        ),
        [],
    )


@pytest.mark.parametrize(
    ["sql_query", "expected_type"],
    [
        ("CREATE TABLE AS SELECT * FROM mytable", OutputTypeDTO.CREATE),
        ("INSERT INTO mytable SELECT * FROM mytable", OutputTypeDTO.APPEND),
        ("insert into mytable select * from mytable", OutputTypeDTO.APPEND),
        ("UPDATE mytable SET a=1", OutputTypeDTO.UPDATE),
        ("DELETE FROM mytable", OutputTypeDTO.DELETE),
        ("COPY mytable FROM '...'", OutputTypeDTO.APPEND),
        ("ALTER TABLE mytable RENAME TO mytable_new", OutputTypeDTO.RENAME),
        ("ALTER TABLE mytable DROP COLUMN a", OutputTypeDTO.ALTER),
        ("TRUNCATE TABLE mytable", OutputTypeDTO.TRUNCATE),
        ("TRUNCATE TABLE mytable DROP STORAGE", OutputTypeDTO.TRUNCATE),
        ("ALTER TABLE mytable TRUNCATE PARTITION (a=1, b=2)", OutputTypeDTO.TRUNCATE),
        ("DROP TABLE mytable", OutputTypeDTO.DROP),
        ("DROP TABLE mytable PURGE", OutputTypeDTO.DROP),
        ("ALTER TABLE mytable DROP PARTITION (a=1, b=2)", OutputTypeDTO.DROP),
        ("MERGE INTO mytable", OutputTypeDTO.MERGE),
        ("CALL myproc()", OutputTypeDTO.UNKNOWN),
    ],
)
def test_extractors_extract_output_batch_with_sql(
    sql_query: str,
    expected_type: OutputTypeDTO,
):
    output = OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
    )
    operation = Mock(spec=OperationDTO)
    operation.sql_query = SQLQueryDTO(query=sql_query)

    event = Mock(spec=OpenLineageRunEvent)
    operation.created_at = event.eventTime = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)

    assert GenericExtractor().extract_output(operation, output, event) == (
        OutputDTO(
            created_at=operation.created_at,
            type=expected_type,
            operation=operation,
            dataset=DatasetDTO(
                name="/user/hive/warehouse/mydb.db/mytable",
                location=LocationDTO(
                    type="hdfs",
                    name="test-hadoop:9820",
                    addresses={"hdfs://test-hadoop:9820"},
                ),
            ),
        ),
        [],
    )


def test_extractors_extract_output_for_long_running_operations():
    output = OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
    )

    # operation is streaming and created long time ago
    operation = Mock(spec=OperationDTO)
    operation.sql_query = None
    operation.created_at = datetime(2024, 7, 5, tzinfo=timezone.utc)

    event = Mock(spec=OpenLineageRunEvent)
    event.eventTime = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)

    assert GenericExtractor().extract_output(operation, output, event) == (
        OutputDTO(
            # count only whole hours since operation was created
            created_at=operation.created_at + timedelta(hours=9),
            type=OutputTypeDTO.UNKNOWN,
            operation=operation,
            dataset=DatasetDTO(
                name="/user/hive/warehouse/mydb.db/mytable",
                location=LocationDTO(
                    type="hdfs",
                    name="test-hadoop:9820",
                    addresses={"hdfs://test-hadoop:9820"},
                ),
            ),
        ),
        [],
    )
