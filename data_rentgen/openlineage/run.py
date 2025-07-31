# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import UUID7, Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets import OpenLineageRunFacets


class OpenLineageRun(OpenLineageBase):
    """Run model.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    runId: UUID7
    facets: OpenLineageRunFacets = Field(default_factory=OpenLineageRunFacets)
