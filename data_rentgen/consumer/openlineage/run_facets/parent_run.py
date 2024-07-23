# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet
from data_rentgen.utils import UUID


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

    runId: UUID


class OpenLineageParentRunFacet(OpenLineageRunFacet):
    """Run facet describing parent run.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    job: OpenLineageParentJob
    run: OpenLineageParentRun
