# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.job import extract_job
from data_rentgen.consumer.extractors.uuid import extract_created_at_from_uuid
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import OpenLineageParentRunFacet
from data_rentgen.dto import RunDTO, RunStatusDTO


def extract_parent_run(parent_facet: OpenLineageParentRunFacet) -> RunDTO:
    return RunDTO(
        created_at=extract_created_at_from_uuid(parent_facet.run.runId),
        id=parent_facet.run.runId,
        job=extract_job(parent_facet.job),
    )


def extract_run(event: OpenLineageRunEvent) -> RunDTO:
    run = RunDTO(
        created_at=extract_created_at_from_uuid(event.run.runId),
        id=event.run.runId,
        job=extract_job(event.job),
    )
    enrich_run_status(run, event)
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