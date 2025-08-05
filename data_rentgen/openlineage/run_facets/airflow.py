# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageAirflowDagInfo(OpenLineageBase):
    """Airflow dag info.
    See [Dag](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    dag_id: str = Field(examples=["my_dag"])
    owner: str | None = Field(default=None, examples=["user:myuser"])


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

    run_id: str = Field(examples=["my_dag_run_id"])
    run_type: OpenLineageAirflowDagRunType
    data_interval_start: datetime
    data_interval_end: datetime


class OpenLineageAirflowTaskGroupInfo(OpenLineageBase):
    """Airflow TaskGroup info.
    See [task_group](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    group_id: str = Field(examples=["my_task_group"])


class OpenLineageAirflowTaskInfo(OpenLineageBase):
    """Airflow task info.
    See [Task](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    task_id: str = Field(examples=["my_task"])
    operator_class: str | None = Field(default=None, examples=["MyOperator"])
    task_group: OpenLineageAirflowTaskGroupInfo | None = None


class OpenLineageAirflowTaskInstanceInfo(OpenLineageBase):
    """Airflow taskInstance info.
    See [TaskInstance](https://github.com/apache/airflow/blob/providers-openlineage/2.2.0/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json).
    """

    try_number: int = Field(examples=[1])
    map_index: int | None = None
    log_url: str | None = Field(
        default=None,
        examples=[
            "http://myairflow.domain.com:8080/log?dag_id=my_dag&task_id=my_task&execution_date=2022-01-01&map_index=0&try_number=1",
        ],
    )


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
