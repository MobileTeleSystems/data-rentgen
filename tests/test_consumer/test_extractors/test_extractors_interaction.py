from __future__ import annotations

from unittest.mock import Mock

import pytest

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
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
from data_rentgen.dto import DatasetDTO, InputDTO, LocationDTO, OperationDTO, OutputDTO, OutputTypeDTO, SchemaDTO


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
    operation_dto = Mock(spec=OperationDTO)

    assert GenericExtractor().extract_input(operation_dto, input) == (
        InputDTO(
            operation=operation_dto,
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
        (None, None, None),
    ],
)
def test_extractors_extract_output(
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
    operation_dto = Mock(spec=OperationDTO)

    assert GenericExtractor().extract_output(operation_dto, output) == (
        OutputDTO(
            type=expected_type,
            operation=operation_dto,
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
