# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageInputDatasetFacet,
)


class OpenLineageInputStatisticsInputDatasetFacet(OpenLineageInputDatasetFacet):
    """Dataset facet describing Input statistics.
    See [InputStatisticsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/InputStatisticsInputDatasetFacet.json).
    """

    rows: int | None = Field(default=None, alias="rowCount", examples=[1_000_000])
    bytes: int | None = Field(default=None, alias="size", examples=[2**30])
    files: int | None = Field(default=None, alias="fileCount", examples=[0])
