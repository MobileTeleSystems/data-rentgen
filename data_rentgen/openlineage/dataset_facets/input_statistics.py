# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field, NonNegativeInt, field_validator

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageInputDatasetFacet,
)

MAX_LONG = 2**63 - 1


class OpenLineageInputStatisticsInputDatasetFacet(OpenLineageInputDatasetFacet):
    """Dataset facet describing Input statistics.
    See [InputStatisticsInputDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/InputStatisticsInputDatasetFacet.json).
    """

    rows: NonNegativeInt | None = Field(default=None, alias="rowCount", examples=[1_000_000])
    bytes: NonNegativeInt | None = Field(default=None, alias="size", examples=[2**30])
    files: NonNegativeInt | None = Field(default=None, alias="fileCount", examples=[0])

    @field_validator("bytes", "rows", "files", mode="after")
    @classmethod
    def value_must_be_sane(cls, value: int | None):
        if value and value >= MAX_LONG:
            # https://github.com/OpenLineage/OpenLineage/pull/4165
            return None
        return value
