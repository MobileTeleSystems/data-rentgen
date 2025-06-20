from __future__ import annotations

from datetime import datetime, timezone

from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import AirflowTaskExtractor
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.consumer.openlineage.run import OpenLineageRun
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskGroupInfo,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
    OpenLineageRunFacets,
)
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
)


def test_extractors_extract_operation_airflow():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("1efc1e7f-4015-6970-b4f9-12e828cb9b91")
    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mydag.mytask",
                location=LocationDTO(
                    type="http",
                    name="airflow-host:8081",
                    addresses={"http://airflow-host:8081"},
                ),
                type=JobTypeDTO(type="AIRFLOW_TASK"),
            ),
            status=RunStatusDTO.SUCCEEDED,
            started_at=None,
            start_reason=RunStartReasonDTO.AUTOMATIC,
            user=None,
            ended_at=now,
            external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
            attempt="1",
            persistent_log_url=(
                "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
            ),
            running_log_url=None,
        ),
        name="mytask",
        type=OperationTypeDTO.BATCH,
        description="BashOperator",
        group=None,
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )


def test_extractors_extract_operation_airflow_task_group():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("1efc1e7f-4015-6970-b4f9-12e828cb9b91")
    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                        task_group=OpenLineageAirflowTaskGroupInfo(group_id="mygroup"),
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mydag.mytask",
                location=LocationDTO(
                    type="http",
                    name="airflow-host:8081",
                    addresses={"http://airflow-host:8081"},
                ),
                type=JobTypeDTO(type="AIRFLOW_TASK"),
            ),
            status=RunStatusDTO.SUCCEEDED,
            started_at=None,
            start_reason=RunStartReasonDTO.AUTOMATIC,
            user=None,
            ended_at=now,
            external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
            attempt="1",
            persistent_log_url=(
                "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
            ),
            running_log_url=None,
        ),
        name="mytask",
        type=OperationTypeDTO.BATCH,
        description="BashOperator",
        group="mygroup",
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )


def test_extractors_extract_operation_airflow_map_index():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("1efc1e7f-4015-6970-b4f9-12e828cb9b91")
    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask_10",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        map_index=10,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mydag.mytask_10",
                location=LocationDTO(
                    type="http",
                    name="airflow-host:8081",
                    addresses={"http://airflow-host:8081"},
                ),
                type=JobTypeDTO(type="AIRFLOW_TASK"),
            ),
            status=RunStatusDTO.SUCCEEDED,
            started_at=None,
            start_reason=RunStartReasonDTO.AUTOMATIC,
            user=None,
            ended_at=now,
            external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
            attempt="1",
            persistent_log_url=(
                "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
            ),
            running_log_url=None,
        ),
        name="mytask",
        type=OperationTypeDTO.BATCH,
        description="BashOperator",
        group=None,
        position=10,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )
