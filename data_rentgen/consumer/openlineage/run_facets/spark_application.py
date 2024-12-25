# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageSparkDeployMode(str, Enum):
    """Spark application deploy mode."""

    CLIENT = "client"
    CLUSTER = "cluster"

    def __str__(self) -> str:
        return self.value


class OpenLineageSparkApplicationDetailsRunFacet(OpenLineageRunFacet):
    """Run facet describing Spark application.
    See [SparkApplicationDetailsFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/SparkApplicationDetailsFacet.java).
    """

    master: str
    appName: str
    applicationId: str
    deployMode: OpenLineageSparkDeployMode
    driverHost: str
    userName: str
    uiWebUrl: str | None = None
    proxyUrl: str | None = None
    historyUrl: str | None = None
