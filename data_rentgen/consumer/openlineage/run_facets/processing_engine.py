# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageProcessingEngineName(str, Enum):
    AIRFLOW = "Airflow"
    SPARK = "spark"

    def __str__(self) -> str:
        return self.value


class OpenLineageProcessingEngineRunFacet(OpenLineageRunFacet):
    """Run facet describing processing engine.
    See [ProcessingEngineRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ProcessingEngineRunFacet.json).
    """

    name: OpenLineageProcessingEngineName
    version: str
    openlineageAdapterVersion: str
