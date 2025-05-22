# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from textwrap import dedent

from data_rentgen.consumer.extractors.run import extract_parent_run, extract_run
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.dto import OperationDTO, OperationStatusDTO, OperationTypeDTO, RunDTO, SQLQueryDTO

"""
SQL_JOB=operation, SPARK_APPLICATION=run
DBT_MODEL=operation, DBT_JOB=run
But AIRFLOW_TASK=operation+run, FLINK_JOB=operation+run
"""
INTEGRATIONS_WITH_RUN_OPERATION_SEPARATION = {"SPARK", "DBT"}


def extract_operation(event: OpenLineageRunEvent) -> OperationDTO:
    run = extract_operation_run(event)

    operation = OperationDTO(
        id=event.run.runId,  # type: ignore [arg-type]
        run=run,
        name=extract_operation_name(run, event),
        type=extract_operation_type(event),
        status=OperationStatusDTO(run.status),
        sql_query=extract_sql_query(event.job),
        started_at=run.started_at,
        ended_at=run.ended_at,
    )
    enrich_operation_status(operation, event)
    enrich_operation_details(operation, event)
    return operation


def extract_operation_run(event: OpenLineageRunEvent) -> RunDTO:
    if (
        event.run.facets.parent
        and event.job.facets.jobType
        and event.job.facets.jobType.integration in INTEGRATIONS_WITH_RUN_OPERATION_SEPARATION
    ):
        return extract_parent_run(event.run.facets.parent)

    return extract_run(event)


def extract_operation_type(event: OpenLineageRunEvent) -> OperationTypeDTO:
    if event.job.facets.jobType and event.job.facets.jobType.processingType:
        return OperationTypeDTO(event.job.facets.jobType.processingType)

    return OperationTypeDTO.BATCH


def extract_operation_name(run: RunDTO, event: OpenLineageRunEvent) -> str | None:
    if event.job.facets.jobType and event.job.facets.jobType.integration == "SPARK":
        # in some cases, operation name may contain raw SELECT query with newlines. use spaces instead
        operation_name = " ".join(line.strip() for line in event.job.name.splitlines()).strip()

        # Spark execution name is "applicationName.operationName". Strip prefix
        if operation_name.startswith(run.job.name) and operation_name != run.job.name:
            prefix = len(run.job.name) + 1
            operation_name = operation_name[prefix:]

        return operation_name

    if event.job.facets.jobType and event.job.facets.jobType.integration == "AIRFLOW":
        airflow_operator_details = event.run.facets.airflow
        if airflow_operator_details:
            return airflow_operator_details.task.task_id

        # for FINISHED/KILLED event we don't receive task facet.
        # keep existing name in DB instead of resetting it to "dag.task"
        return None

    return event.job.name


def enrich_operation_status(operation: OperationDTO, event: OpenLineageRunEvent) -> OperationDTO:
    match event.eventType:
        case OpenLineageRunEventType.START:
            operation.started_at = event.eventTime
            operation.status = OperationStatusDTO.STARTED
        case OpenLineageRunEventType.RUNNING:
            operation.status = OperationStatusDTO.STARTED
        case OpenLineageRunEventType.COMPLETE:
            operation.ended_at = event.eventTime
            operation.status = OperationStatusDTO.SUCCEEDED
        case OpenLineageRunEventType.FAIL:
            operation.ended_at = event.eventTime
            operation.status = OperationStatusDTO.FAILED
        case OpenLineageRunEventType.ABORT:
            operation.ended_at = event.eventTime
            operation.status = OperationStatusDTO.KILLED
        case OpenLineageRunEventType.OTHER:
            # OTHER is used only to update run statistics
            pass
    return operation


def enrich_operation_details(operation: OperationDTO, event: OpenLineageRunEvent) -> OperationDTO:
    spark_job_details = event.run.facets.spark_jobDetails
    if spark_job_details:
        operation.position = spark_job_details.jobId
        operation.group = spark_job_details.jobGroup
        operation.description = spark_job_details.jobDescription

    airflow_operator_details = event.run.facets.airflow
    if airflow_operator_details:
        operation.description = airflow_operator_details.task.operator_class
        operation.position = airflow_operator_details.taskInstance.map_index
        if airflow_operator_details.task.task_group:
            operation.group = airflow_operator_details.task.task_group.group_id

    if event.job.facets.jobType and event.job.facets.jobType.integration == "DBT":
        operation.group = event.job.facets.jobType.jobType
    return operation


def extract_sql_query(job: OpenLineageJob) -> SQLQueryDTO | None:
    """
    Sql queries are usual has format of multiline string. So we remove additional spaces and end of the rows symbols.
    """
    if job.facets.sql:
        query = dedent(job.facets.sql.query).strip()
        return SQLQueryDTO(query=query)

    return None
