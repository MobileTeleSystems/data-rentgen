# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import quote

from packaging.version import Version

from data_rentgen.consumer.extractors import extract_job
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskRunFacet,
    OpenLineageParentRunFacet,
)
from data_rentgen.dto import RunDTO, RunStartReasonDTO, RunStatusDTO


def extract_parent_run(parent_facet: OpenLineageParentRunFacet) -> RunDTO:
    return RunDTO(
        id=parent_facet.run.runId,  # type: ignore [arg-type]
        job=extract_job(parent_facet.job),
    )


def extract_run(event: OpenLineageRunEvent) -> RunDTO:
    run = RunDTO(
        id=event.run.runId,  # type: ignore [arg-type]
        job=extract_job(event.job),
    )
    enrich_run_status(run, event)
    enrich_run_start_reason(run, event)
    enrich_run_identifiers(run, event)
    enrich_run_logs(run, event)
    return run


def enrich_run_status(run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
    match event.eventType:
        case OpenLineageRunEventType.START:
            run.started_at = event.eventTime
            run.status = RunStatusDTO.STARTED
        case OpenLineageRunEventType.RUNNING:
            run.status = RunStatusDTO.STARTED
        case OpenLineageRunEventType.COMPLETE:
            run.ended_at = event.eventTime
            run.status = RunStatusDTO.SUCCEEDED
        case OpenLineageRunEventType.FAIL:
            run.ended_at = event.eventTime
            run.status = RunStatusDTO.FAILED
        case OpenLineageRunEventType.ABORT:
            run.ended_at = event.eventTime
            run.status = RunStatusDTO.KILLED
        case OpenLineageRunEventType.OTHER:
            # OTHER is used only to update run statistics
            pass
    return run


def enrich_run_identifiers(run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
    spark_application_details = event.run.facets.spark_applicationDetails
    if spark_application_details:
        run.external_id = spark_application_details.applicationId

    airflow_dag_run_facet = event.run.facets.airflowDagRun
    if airflow_dag_run_facet:
        run.external_id = airflow_dag_run_facet.dagRun.run_id

    airflow_task_run_facet = event.run.facets.airflow
    if airflow_task_run_facet:
        run.external_id = airflow_task_run_facet.dagRun.run_id
        run.attempt = str(airflow_task_run_facet.taskInstance.try_number)
    return run


def enrich_run_logs(run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
    spark_application_details = event.run.facets.spark_applicationDetails
    if spark_application_details:
        run.running_log_url = spark_application_details.proxyUrl or spark_application_details.uiWebUrl
        run.persistent_log_url = spark_application_details.historyUrl
        return run

    airflow_dag_run_facet = event.run.facets.airflowDagRun
    if airflow_dag_run_facet:
        namespace = event.job.namespace
        if not namespace.startswith("http"):
            return run

        processing_engine = event.run.facets.processing_engine
        if processing_engine and processing_engine.version >= Version("2.3.0"):
            run.persistent_log_url = get_airflow_2_3_plus_dag_run_url(namespace, airflow_dag_run_facet)
        else:
            run.persistent_log_url = get_airflow_2_x_dag_run_url(namespace, airflow_dag_run_facet)

    airflow_task_run_facet = event.run.facets.airflow
    if airflow_task_run_facet:
        # https://github.com/OpenLineage/OpenLineage/pull/2852
        if airflow_task_run_facet.taskInstance.log_url:
            run.persistent_log_url = airflow_task_run_facet.taskInstance.log_url
            return run

        namespace = event.job.namespace
        if not namespace.startswith("http"):
            return run

        processing_engine = event.run.facets.processing_engine
        if processing_engine and processing_engine.version >= Version("2.9.1"):
            run.persistent_log_url = get_airflow_2_9_plus_task_log_url(namespace, airflow_task_run_facet)
        else:
            run.persistent_log_url = get_airflow_2_x_task_log_url(namespace, airflow_task_run_facet)
    return run


def get_airflow_2_3_plus_dag_run_url(  # noqa: WPS114
    namespace: str,
    airflow_dag_run_facet: OpenLineageAirflowDagRunFacet,
) -> str:
    # https://github.com/apache/airflow/pull/20730
    # https://github.com/apache/airflow/blob/2.9.2/airflow/www/views.py#L2788
    dag_id = airflow_dag_run_facet.dag.dag_id
    dag_run_id = quote(airflow_dag_run_facet.dagRun.run_id)
    return f"{namespace}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"


def get_airflow_2_x_dag_run_url(  # noqa: WPS114
    namespace: str,
    airflow_dag_run_facet: OpenLineageAirflowDagRunFacet,
) -> str:
    # https://github.com/apache/airflow/blob/2.9.2/airflow/www/views.py#L2975
    dag_id = quote(airflow_dag_run_facet.dag.dag_id)
    execution_date = quote(airflow_dag_run_facet.dagRun.data_interval_start.isoformat())
    return f"{namespace}/graph?dag_id={dag_id}&execution_date={execution_date}"


def get_airflow_2_9_plus_task_log_url(  # noqa: WPS114
    namespace: str,
    airflow_task_run_facet: OpenLineageAirflowTaskRunFacet,
) -> str:
    # https://github.com/apache/airflow/pull/39183
    # https://github.com/apache/airflow/blob/2.9.1/airflow/models/taskinstance.py#L1720-L1734
    dag_id = airflow_task_run_facet.dag.dag_id
    dag_run_id = quote(airflow_task_run_facet.dagRun.run_id)
    task_id = quote(airflow_task_run_facet.task.task_id)
    map_index = airflow_task_run_facet.taskInstance.map_index
    if map_index is None:
        map_index = -1
    return f"{namespace}/dags/{dag_id}/grid?tab=logs&dag_run_id={dag_run_id}&task_id={task_id}&map_index={map_index}"


def get_airflow_2_x_task_log_url(  # noqa: WPS114
    namespace: str,
    airflow_task_run_facet: OpenLineageAirflowTaskRunFacet,
) -> str:
    # https://github.com/apache/airflow/blob/2.1.0/airflow/models/taskinstance.py#L524-L528
    dag_id = quote(airflow_task_run_facet.dag.dag_id)
    execution_date = quote(airflow_task_run_facet.dagRun.data_interval_start.isoformat())
    task_id = quote(airflow_task_run_facet.task.task_id)
    return f"{namespace}/log?&dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"


def enrich_run_start_reason(run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
    airflow_run_facet = event.run.facets.airflow or event.run.facets.airflowDagRun
    if airflow_run_facet:
        if airflow_run_facet.dagRun.run_type == OpenLineageAirflowDagRunType.MANUAL:
            run.start_reason = RunStartReasonDTO.MANUAL
        else:
            run.start_reason = RunStartReasonDTO.AUTOMATIC
    # For Spark session we cannot determine start reason
    return run
