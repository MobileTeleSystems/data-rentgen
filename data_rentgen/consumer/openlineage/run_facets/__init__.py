# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets.airflow import (
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskGroupInfo,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
)
from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet
from data_rentgen.consumer.openlineage.run_facets.dbt_run import OpenLineageDbtRunRunFacet
from data_rentgen.consumer.openlineage.run_facets.flink_job import (
    OpenLineageFlinkJobDetailsRunFacet,
)
from data_rentgen.consumer.openlineage.run_facets.hive_query import OpenLineageHiveQueryInfoRunFacet
from data_rentgen.consumer.openlineage.run_facets.hive_session import OpenLineageHiveSessionInfoRunFacet
from data_rentgen.consumer.openlineage.run_facets.parent_run import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
)
from data_rentgen.consumer.openlineage.run_facets.processing_engine import (
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
    "OpenLineageAirflowDagInfo",
    "OpenLineageAirflowDagRunFacet",
    "OpenLineageAirflowDagRunInfo",
    "OpenLineageAirflowDagRunType",
    "OpenLineageAirflowTaskGroupInfo",
    "OpenLineageAirflowTaskInfo",
    "OpenLineageAirflowTaskInstanceInfo",
    "OpenLineageAirflowTaskRunFacet",
    "OpenLineageDbtRunRunFacet",
    "OpenLineageFlinkJobDetailsRunFacet",
    "OpenLineageParentJob",
    "OpenLineageParentRun",
    "OpenLineageParentRunFacet",
    "OpenLineageProcessingEngineRunFacet",
    "OpenLineageRunFacet",
    "OpenLineageRunFacets",
    "OpenLineageSparkApplicationDetailsRunFacet",
    "OpenLineageSparkDeployMode",
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
    dbt_run: OpenLineageDbtRunRunFacet | None = None
    flink_job: OpenLineageFlinkJobDetailsRunFacet | None = None
    hive_query: OpenLineageHiveQueryInfoRunFacet | None = None
    hive_session: OpenLineageHiveSessionInfoRunFacet | None = None
