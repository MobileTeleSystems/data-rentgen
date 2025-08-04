# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


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

    master: str = Field(examples=["yarn"])
    appName: str = Field(examples=["spark_session"])
    applicationId: str = Field(examples=["application_1234_1234"])
    deployMode: OpenLineageSparkDeployMode
    driverHost: str = Field(examples=["192.168.1.15"])
    userName: str = Field(examples=["user"])
    uiWebUrl: str | None = Field(default=None, examples=["http://192.168.1.15:4040"])
    proxyUrl: str | None = Field(
        default=None,
        examples=["http://mycluster-nn1.domain.com:8080/proxy/application_1234_1234"],
    )
    historyUrl: str | None = Field(
        default=None,
        examples=["http://mycluster-nn1.domain.com:18081/history/application_1234_1234"],
    )
