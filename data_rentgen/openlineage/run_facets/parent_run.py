# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import UUID7

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageParentJob(OpenLineageBase):
    """Parent job identifier.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    namespace: str
    name: str


class OpenLineageParentRun(OpenLineageBase):
    """Parent run identifier.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    runId: UUID7


class OpenLineageParentRunFacet(OpenLineageRunFacet):
    """Run facet describing parent run.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    job: OpenLineageParentJob
    run: OpenLineageParentRun
