# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageOutputDatasetFacet,
)


class OpenLineageOutputStatisticsOutputDatasetFacet(OpenLineageOutputDatasetFacet):
    """Dataset facet describing output statistics.
    See [OutputStatisticsOutputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/OutputStatisticsOutputDatasetFacet.json).
    """

    rowCount: int | None = None
    size: int | None = None
    fileCount: int | None = None
