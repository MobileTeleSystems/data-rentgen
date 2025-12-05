# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageColumnLineageDatasetFacetFieldTransformation(OpenLineageBase):
    """Dataset facet describing field transformation."""

    type: str = Field(examples=["DIRECT", "INDIRECT"])
    subtype: str | None = Field(
        default=None,
        examples=["TRANSFORMATION", "AGGREGATION", "JOIN", "FILTER", "WINDOW", "SORT"],
    )
    description: str | None = None
    masking: bool = False


class OpenLineageColumnLineageDatasetFacetFieldRef(OpenLineageBase):
    """Dataset facet describing field reference for column lineage facet."""

    namespace: str = Field(examples=["hive://rnd-dwh"], json_schema_extra={"format": "uri"})
    name: str = Field(examples=["otherdb.othertable"])
    field: str = Field(examples=["col1", "col2"])
    transformations: list[OpenLineageColumnLineageDatasetFacetFieldTransformation] = Field(default_factory=list)


class OpenLineageColumnLineageDatasetFacetField(OpenLineageBase):
    """Dataset facet describing column lineage for specific field."""

    inputFields: list[OpenLineageColumnLineageDatasetFacetFieldRef] = Field(default_factory=list)
    transformationDescription: str | None = Field(default=None, deprecated=True)
    transformationType: str | None = Field(default=None, deprecated=True)


class OpenLineageColumnLineageDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing column lineage.
    See [InputStatisticsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ColumnLineageDatasetFacet.json).
    """

    fields: dict[str, OpenLineageColumnLineageDatasetFacetField] = Field(default_factory=dict)
    dataset: list[OpenLineageColumnLineageDatasetFacetFieldRef] = Field(default_factory=list)
