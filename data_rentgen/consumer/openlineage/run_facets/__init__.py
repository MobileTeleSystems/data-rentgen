# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets.airflow import (
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
)
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
    "OpenLineageRunFacets",
    "OpenLineageAirflowTaskInstanceInfo",
    "OpenLineageAirflowTaskInfo",
    "OpenLineageAirflowTaskRunFacet",
    "OpenLineageAirflowDagRunInfo",
    "OpenLineageAirflowDagRunType",
    "OpenLineageAirflowDagInfo",
    "OpenLineageAirflowDagRunFacet",
    "OpenLineageSparkDeployMode",
    "OpenLineageSparkApplicationDetailsRunFacet",
    "OpenLineageSparkJobDetailsRunFacet",
]


class OpenLineageRunFacets(OpenLineageBase):
    """All possible run facets.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    parent: OpenLineageParentRunFacet | None = None
    processing_engine: OpenLineageProcessingEngineRunFacet | None = None
    spark_applicationDetails: OpenLineageSparkApplicationDetailsRunFacet | None = None
    spark_jobDetails: OpenLineageSparkJobDetailsRunFacet | None = None
    airflow: OpenLineageAirflowTaskRunFacet | None = None
    airflowDagRun: OpenLineageAirflowDagRunFacet | None = None
