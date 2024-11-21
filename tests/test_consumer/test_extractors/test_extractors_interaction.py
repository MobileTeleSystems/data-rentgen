from unittest.mock import Mock

import pytest

from data_rentgen.consumer.extractors import (
    extract_input,
    extract_output,
    extract_schema,
)
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
from data_rentgen.dto import InputDTO, OutputDTO, OutputTypeDTO, SchemaDTO
from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.location import LocationDTO
from data_rentgen.dto.operation import OperationDTO


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
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
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

    assert extract_schema(dataset) == SchemaDTO(
        fields=[
            {"name": "dt", "type": "timestamp", "description": "Business date"},
            {"name": "customer_id", "type": "decimal(20,0)"},
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

    assert extract_input(operation_dto, input) == InputDTO(
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
    )


@pytest.mark.parametrize(
    ["lifecycle_state_change", "expected_type"],
    [
        (OpenLineageDatasetLifecycleStateChange.CREATE, OutputTypeDTO.CREATE),
        (OpenLineageDatasetLifecycleStateChange.OVERWRITE, OutputTypeDTO.OVERWRITE),
        (OpenLineageDatasetLifecycleStateChange.ALTER, OutputTypeDTO.ALTER),
        (OpenLineageDatasetLifecycleStateChange.CREATE, OutputTypeDTO.CREATE),
        (OpenLineageDatasetLifecycleStateChange.DROP, OutputTypeDTO.DROP),
        (OpenLineageDatasetLifecycleStateChange.OVERWRITE, OutputTypeDTO.OVERWRITE),
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

    assert extract_output(operation_dto, output) == OutputDTO(
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
    )
