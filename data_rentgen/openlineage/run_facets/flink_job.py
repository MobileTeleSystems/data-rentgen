# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageFlinkJobDetailsRunFacet(OpenLineageRunFacet):
    """Run facet describing Flink job.
    See [FlinkJobDetailsFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/flink/shared/src/main/java/io/openlineage/flink/facets/FlinkJobDetailsFacet.java).
    """

    jobId: str = Field(examples=["44f7bc13-4538-42c7-a5be-8edb36c39a45"])
