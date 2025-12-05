# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageSparkJobDetailsRunFacet(OpenLineageRunFacet):
    """Run facet describing Spark job.
    See [SparkJobDetailsFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/SparkJobDetailsFacet.java).
    """

    jobId: int
    jobDescription: str | None = None
    jobGroup: str | None = None
    jobCallSite: str | None = None
