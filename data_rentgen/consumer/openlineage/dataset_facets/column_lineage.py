# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageColumnLineageDatasetFacetFieldTransformation(OpenLineageBase):
    """Dataset facet describing field transformation."""

    type: str
    subtype: str | None = None
    description: str | None = None
    masking: bool = False


class OpenLineageColumnLineageDatasetFacetFieldRef(OpenLineageBase):
    """Dataset facet describing field reference for column lineage facet."""

    namespace: str
    name: str
    field: str
    transformations: list[OpenLineageColumnLineageDatasetFacetFieldTransformation] = Field(default_factory=list)


class OpenLineageColumnLineageDatasetFacetField(OpenLineageBase):
    """Dataset facet describing column lineage for specific field."""

    inputFields: list[OpenLineageColumnLineageDatasetFacetFieldRef] = Field(default_factory=list)
    transformationDescription: str | None = None
    transformationType: str | None = None


class OpenLineageColumnLineageDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing column lineage.
    See [InputStatisticsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ColumnLineageDatasetFacet.json).
    """

    fields: dict[str, OpenLineageColumnLineageDatasetFacetField] = Field(default_factory=dict)
    dataset: list[OpenLineageColumnLineageDatasetFacetFieldRef] = Field(default_factory=list)
