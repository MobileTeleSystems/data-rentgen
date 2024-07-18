import pytest

from data_rentgen.consumer.extractors import (
    extract_input_interaction,
    extract_interaction_schema,
    extract_output_interaction,
)
from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDataQualityMetricsInputDatasetFacet,
    OpenLineageDatasetFacets,
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageInputDatasetFacets,
    OpenLineageLifecycleStateChangeDatasetFacet,
    OpenLineageOutputDatasetFacets,
    OpenLineageOutputStatisticsOutputDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.dto import InteractionDTO, InteractionTypeDTO, SchemaDTO


@pytest.mark.parametrize("dataset_type", [OpenLineageInputDataset, OpenLineageOutputDataset])
def test_extractors_extract_interaction_schema(dataset_type: type[OpenLineageDataset]):
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

    assert extract_interaction_schema(dataset) == SchemaDTO(
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
def test_extractors_extract_input_interaction(
    row_count: int | None,
    byte_count: int | None,
    file_count: int | None,
):
    input = OpenLineageInputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        inputFacets=OpenLineageInputDatasetFacets(
            dataQualityMetrics=OpenLineageDataQualityMetricsInputDatasetFacet(
                rowCount=row_count,
                bytes=byte_count,
                fileCount=file_count,
            ),
        ),
    )

    assert extract_input_interaction(input) == InteractionDTO(
        type=InteractionTypeDTO.READ,
        num_rows=row_count,
        num_bytes=byte_count,
        num_files=file_count,
    )


@pytest.mark.parametrize(
    ["lifecycle_state_change", "expected_interaction_type"],
    [
        (OpenLineageDatasetLifecycleStateChange.CREATE, InteractionTypeDTO.CREATE),
        (OpenLineageDatasetLifecycleStateChange.OVERWRITE, InteractionTypeDTO.OVERWRITE),
        (OpenLineageDatasetLifecycleStateChange.ALTER, InteractionTypeDTO.ALTER),
        (OpenLineageDatasetLifecycleStateChange.CREATE, InteractionTypeDTO.CREATE),
        (OpenLineageDatasetLifecycleStateChange.DROP, InteractionTypeDTO.DROP),
        (OpenLineageDatasetLifecycleStateChange.OVERWRITE, InteractionTypeDTO.OVERWRITE),
        (OpenLineageDatasetLifecycleStateChange.RENAME, InteractionTypeDTO.RENAME),
        (OpenLineageDatasetLifecycleStateChange.TRUNCATE, InteractionTypeDTO.TRUNCATE),
    ],
)
@pytest.mark.parametrize(
    ["row_count", "byte_count", "file_count"],
    [
        (1_000_000, 1000 * 1024 * 1024, 10),
        (None, None, None),
    ],
)
def test_extractors_extract_output_interaction(
    lifecycle_state_change: OpenLineageDatasetLifecycleStateChange,
    expected_interaction_type: InteractionTypeDTO,
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

    assert extract_output_interaction(output) == InteractionDTO(
        type=expected_interaction_type,
        num_rows=row_count,
        num_bytes=byte_count,
        num_files=file_count,
    )