# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageFlinkJobDetailsRunFacet(OpenLineageRunFacet):
    """Run facet describing Flink job.
    See [FlinkJobDetailsFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/flink/shared/src/main/java/io/openlineage/flink/facets/FlinkJobDetailsFacet.java).
    """

    jobId: str
