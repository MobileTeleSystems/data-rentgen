# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageSchemaField(OpenLineageBase):
    """Dataset field information.
    See [SchemaDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SchemaDatasetFacet.json).
    """

    name: str
    type: str | None = None
    description: str | None = None
    fields: list[OpenLineageSchemaField] | None = None


class OpenLineageSchemaDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing schema.
    See [SchemaDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SchemaDatasetFacet.json).
    """

    fields: list[OpenLineageSchemaField]
