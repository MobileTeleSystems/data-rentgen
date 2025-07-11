# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class DataRentgenOperationInfoFacet(OpenLineageRunFacet):
    """
    Custom facet representing DataRentgen Operation fields.

    If OpenLineage Run have this facet, it is considered a DataRentgen Operation,
    or DataRentgen Run + Operation if DataRentgenRunInfoFacet is also present.
    """

    name: str | None = None
    description: str | None = None
    group: str | None = None
    position: int | None = None
