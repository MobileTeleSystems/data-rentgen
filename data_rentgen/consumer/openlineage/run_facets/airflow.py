# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageAirflowDagInfo(OpenLineageBase):
    """Airflow dag info.
    See [Dag](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    dag_id: str
    owner: str | None = None


class OpenLineageAirflowDagRunType(Enum):
    """Airflow dagRun type.
    See [DagRunType](https://github.com/apache/airflow/blob/2.9.3/airflow/utils/types.py#L48-L54).
    """

    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    DATASET_TRIGGERED = "dataset_triggered"


class OpenLineageAirflowDagRunInfo(OpenLineageBase):
    """Airflow dagRun info.
    See [DagRun](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    run_id: str
    run_type: OpenLineageAirflowDagRunType
    data_interval_start: datetime
    data_interval_end: datetime


class OpenLineageAirflowTaskGroupInfo(OpenLineageBase):
    """Airflow TaskGroup info.
    See [task_group](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    group_id: str


class OpenLineageAirflowTaskInfo(OpenLineageBase):
    """Airflow task info.
    See [Task](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    task_id: str
    operator_class: str | None = None
    task_group: OpenLineageAirflowTaskGroupInfo | None = None


class OpenLineageAirflowTaskInstanceInfo(OpenLineageBase):
    """Airflow taskInstance info.
    See [TaskInstance](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    try_number: int
    map_index: int | None = None
    log_url: str | None = None


class OpenLineageAirflowDagRunFacet(OpenLineageRunFacet):
    """Run facet describing Airflow DAG run.
    See [AirflowDagRunFacet](https://github.com/apache/airflow/blob/main/airflow/providers/openlineage/facets/AirflowDagRunFacet.json).
    """

    dag: OpenLineageAirflowDagInfo
    dagRun: OpenLineageAirflowDagRunInfo


class OpenLineageAirflowTaskRunFacet(OpenLineageRunFacet):
    """Run facet describing Airflow Task run.
    See [AirflowRunFacet](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    dag: OpenLineageAirflowDagInfo
    dagRun: OpenLineageAirflowDagRunInfo
    task: OpenLineageAirflowTaskInfo
    taskInstance: OpenLineageAirflowTaskInstanceInfo
