# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageAirflowDagInfo(OpenLineageBase):
    """Airflow dag info.
    See [Dag](https://github.com/apache/airflow/blob/providers-openlineage/1.9.0/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    dag_id: str


class OpenLineageAirflowDagRunInfo(OpenLineageBase):
    """Airflow dagRun info.
    See [DagRun](https://github.com/apache/airflow/blob/providers-openlineage/1.9.0/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    run_id: str
    data_interval_start: datetime
    data_interval_end: datetime


class OpenLineageAirflowTaskInfo(OpenLineageBase):
    """Airflow task info.
    See [Task](https://github.com/apache/airflow/blob/providers-openlineage/1.9.0/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    task_id: str


class OpenLineageAirflowTaskInstanceInfo(OpenLineageBase):
    """Airflow taskInstance info.
    See [TaskInstance](https://github.com/apache/airflow/blob/providers-openlineage/1.9.0/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    try_number: int
    map_index: int | None = None
    log_url: str | None = None


class OpenLineageAirflowRunFacet(OpenLineageRunFacet):
    """Run facet describing Airflow run.
    See [AirflowRunFacet](https://github.com/apache/airflow/blob/providers-openlineage/1.9.0/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    dag: OpenLineageAirflowDagInfo
    dagRun: OpenLineageAirflowDagRunInfo
    task: OpenLineageAirflowTaskInfo
    taskInstance: OpenLineageAirflowTaskInstanceInfo
