# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class DataRentgenOperationInfoFacet(OpenLineageRunFacet):
    """
    Custom facet representing DataRentgen Operation fields.

    If OpenLineage Run have this facet, it is considered a DataRentgen Operation,
    or DataRentgen Run + Operation if DataRentgenRunInfoFacet is also present.
    """

    name: str | None = Field(default=None, examples=["execute_save_into_data_source_command"])
    description: str | None = Field(default=None, examples=["Clickhouse -> Clickhouse"])
    group: str | None = Field(default=None, examples=["my_group"])
    position: int | None = Field(default=None, examples=[1])
