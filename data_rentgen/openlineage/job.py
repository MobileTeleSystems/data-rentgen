# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.job_facets import OpenLineageJobFacets


class OpenLineageJob(OpenLineageBase):
    """Job model.
    See [Job](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    namespace: str = Field(examples=["yarn://rnd-dwh"], json_schema_extra={"format": "uri"})
    name: str = Field(examples=["my_spark_session"])
    facets: OpenLineageJobFacets = Field(default_factory=OpenLineageJobFacets)
