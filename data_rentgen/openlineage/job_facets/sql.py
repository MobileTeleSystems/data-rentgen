# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import Field

from data_rentgen.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageSqlJobFacet(OpenLineageJobFacet):
    query: str = Field(examples=["SELECT * FROM my_table"])
