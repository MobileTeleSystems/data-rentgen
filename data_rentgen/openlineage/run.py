# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets import OpenLineageRunFacets
from data_rentgen.utils import UUIDv6Plus


class OpenLineageRun(OpenLineageBase):
    """Run model.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    runId: UUIDv6Plus
    facets: OpenLineageRunFacets = Field(default_factory=OpenLineageRunFacets)
