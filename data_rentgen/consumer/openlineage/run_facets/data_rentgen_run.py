# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class DataRentgenRunInfoFacet(OpenLineageRunFacet):
    """
    Custom facet representing DataRentgen Run fields.

    If OpenLineage Run have this facet, it is considered a DataRentgen Run,
    or DataRentgen Run + Operation if DataRentgenOperationInfoFacet is also present.
    """

    external_id: str | None = None
    attempt: str | None = None
    running_log_url: str | None = None
    persistent_log_url: str | None = None
    start_reason: Literal["AUTOMATIC", "MANUAL"] | None = None
    started_by_user: str | None = None
