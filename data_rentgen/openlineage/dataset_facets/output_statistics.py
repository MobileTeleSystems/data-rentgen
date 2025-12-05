# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field, NonNegativeInt

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageOutputDatasetFacet,
)


class OpenLineageOutputStatisticsOutputDatasetFacet(OpenLineageOutputDatasetFacet):
    """Dataset facet describing output statistics.
    See [OutputStatisticsOutputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/OutputStatisticsOutputDatasetFacet.json).
    """

    rows: NonNegativeInt | None = Field(default=None, alias="rowCount", examples=[1_000_000])
    bytes: NonNegativeInt | None = Field(default=None, alias="size", examples=[2**30])
    files: NonNegativeInt | None = Field(default=None, alias="fileCount", examples=[0])
