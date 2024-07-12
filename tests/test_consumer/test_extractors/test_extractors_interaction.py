import pytest

from data_rentgen.consumer.extractors import extract_interaction_schema
from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.dto import SchemaDTO


@pytest.mark.parametrize(
    "dataset_type",
    [OpenLineageInputDataset, OpenLineageOutputDataset],
)
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
