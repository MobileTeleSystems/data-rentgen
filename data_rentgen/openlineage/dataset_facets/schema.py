# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageSchemaField(OpenLineageBase):
    """Dataset field information.
    See [SchemaDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SchemaDatasetFacet.json).
    """

    name: str = Field(examples=["id", "name"])
    type: str | None = Field(default=None, examples=["INT", "VARCHAR"])
    description: str | None = None
    fields: list[OpenLineageSchemaField] | None = Field(
        default=None,
        description="Nested fields, if any",
        examples=[[]],
    )


class OpenLineageSchemaDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing schema.
    See [SchemaDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SchemaDatasetFacet.json).
    """

    fields: list[OpenLineageSchemaField]
