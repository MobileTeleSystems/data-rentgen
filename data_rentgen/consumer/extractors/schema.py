# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.dataset_facets import OpenLineageSchemaField
from data_rentgen.dto.schema import SchemaDTO


def field_to_json(field: OpenLineageSchemaField):
    result: dict = {
        "name": field.name,
    }
    if field.type:
        result["type"] = field.type

    description_ = (field.description or "").strip()
    if description_:
        result["description"] = description_

    if field.fields:
        result["fields"] = [field_to_json(f) for f in field.fields]
    return result


def extract_schema(dataset: OpenLineageDataset) -> SchemaDTO | None:
    if not dataset.facets.datasetSchema:
        return None

    fields = dataset.facets.datasetSchema.fields
    if not fields:
        return None

    return SchemaDTO(
        fields=[field_to_json(field) for field in fields],
    )
