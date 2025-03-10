# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from uuid import UUID

from msgspec import field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets import OpenLineageRunFacets


class OpenLineageRun(OpenLineageBase):
    """Run model.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    runId: UUID
    facets: OpenLineageRunFacets = field(default_factory=OpenLineageRunFacets)
