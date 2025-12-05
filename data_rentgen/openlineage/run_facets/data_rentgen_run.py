# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class DataRentgenRunInfoFacet(OpenLineageRunFacet):
    """
    Custom facet representing DataRentgen Run fields.

    If OpenLineage Run have this facet, it is considered a DataRentgen Run,
    or DataRentgen Run + Operation if DataRentgenOperationInfoFacet is also present.
    """

    external_id: str | None = Field(default=None, examples=["application_1234_1234"])
    attempt: str | None = Field(default=None, examples=["1"])
    running_log_url: str | None = Field(default=None, examples=["http://192.168.1.15:4040"])
    persistent_log_url: str | None = Field(
        default=None,
        examples=["http://mycluster-nn1.domain.com:18081/history/application_1234_1234"],
    )
    start_reason: Literal["AUTOMATIC", "MANUAL"] | None = Field(default=None, examples=["AUTOMATIC"])
    started_by_user: str | None = Field(default=None, examples=["user"])
