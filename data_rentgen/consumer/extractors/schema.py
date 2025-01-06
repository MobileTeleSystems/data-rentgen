# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset import OpenLineageDataset
from data_rentgen.dto.schema import SchemaDTO


def extract_schema(dataset: OpenLineageDataset) -> SchemaDTO | None:
    if not dataset.facets.datasetSchema:
        return None

    fields = dataset.facets.datasetSchema.fields
    return SchemaDTO(
        fields=[field.model_dump(exclude_unset=True, exclude_none=True) for field in fields],
    )
