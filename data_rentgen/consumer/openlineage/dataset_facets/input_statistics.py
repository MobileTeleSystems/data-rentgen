# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageInputDatasetFacet,
)


class OpenLineageInputStatisticsInputDatasetFacet(OpenLineageInputDatasetFacet):
    """Dataset facet describing Input statistics.
    See [InputStatisticsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/InputStatisticsInputDatasetFacet.json).
    """

    rowCount: int | None = None
    size: int | None = None
    fileCount: int | None = None
