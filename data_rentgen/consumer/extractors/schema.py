# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.consumer.openlineage.dataset_facets import OpenLineageSchemaField
from data_rentgen.dto.schema import SchemaDTO


def field_to_dict_compact(field: OpenLineageSchemaField):
    result: dict[str, Any] = {
        "name": field.name,
    }
    if field.type:
        result["type"] = field.type
    if field.description:
        result["description"] = field.description
    if field.fields:
        result["fields"] = list(map(field_to_dict_compact, field.fields))
    return result


def extract_schema(dataset: OpenLineageDataset) -> SchemaDTO | None:
    if not dataset.facets.schema:
        return None

    fields = dataset.facets.schema.fields
    return SchemaDTO(
        fields=list(map(field_to_dict_compact, fields)),
    )
