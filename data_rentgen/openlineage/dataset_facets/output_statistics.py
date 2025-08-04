# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageOutputDatasetFacet,
)


class OpenLineageOutputStatisticsOutputDatasetFacet(OpenLineageOutputDatasetFacet):
    """Dataset facet describing output statistics.
    See [OutputStatisticsOutputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/OutputStatisticsOutputDatasetFacet.json).
    """

    rows: int | None = Field(default=None, alias="rowCount", examples=[1_000_000])
    bytes: int | None = Field(default=None, alias="size", examples=[2**30])
    files: int | None = Field(default=None, alias="fileCount", examples=[0])
