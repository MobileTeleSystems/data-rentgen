# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TypedDict

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet
from data_rentgen.consumer.openlineage.run_facets.parent_run import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
)
from data_rentgen.consumer.openlineage.run_facets.processing_engine import (
    OpenLineageProcessingEngineName,
    OpenLineageProcessingEngineRunFacet,
)
from data_rentgen.consumer.openlineage.run_facets.spark_application import (
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.consumer.openlineage.run_facets.spark_job import (
    OpenLineageSparkJobDetailsRunFacet,
)

__all__ = [
    "OpenLineageRunFacet",
    "OpenLineageParentRunFacet",
    "OpenLineageParentJob",
    "OpenLineageParentRun",
    "OpenLineageProcessingEngineName",
    "OpenLineageProcessingEngineRunFacet",
    "OpenLineageRunFacetsDict",
    "OpenLineageSparkDeployMode",
    "OpenLineageSparkApplicationDetailsRunFacet",
    "OpenLineageSparkJobDetailsRunFacet",
]


class OpenLineageRunFacetsDict(TypedDict, total=False):
    """All possible run facets.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    parent: OpenLineageParentRunFacet
    processing_engine: OpenLineageProcessingEngineRunFacet
    spark_applicationDetails: OpenLineageSparkApplicationDetailsRunFacet
    spark_jobDetails: OpenLineageSparkJobDetailsRunFacet
