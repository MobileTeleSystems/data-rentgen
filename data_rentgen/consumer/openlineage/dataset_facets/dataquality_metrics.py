# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageOutputDatasetFacet,
)


class OpenLineageDataQualityMetricsInputDatasetFacet(OpenLineageOutputDatasetFacet):
    """Dataset facet describing data quality metrics.
    See [DataQualityMetricsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DataQualityMetricsInputDatasetFacet.json).

    Note: `columnMetrics` are ignored.
    """

    rows: int | None = Field(default=None, alias="rowCount")
    bytes: int | None = None
    files: int | None = Field(default=None, alias="fileCount")
