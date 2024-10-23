# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.run import extract_run_minimal
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.dto import OperationDTO, OperationStatusDTO, OperationTypeDTO


def extract_operation(event: OpenLineageRunEvent) -> OperationDTO:
    # operation always has parent
    run = extract_run_minimal(event.run.facets.parent)  # type: ignore[arg-type]

    # in some cases, operation name may contain raw SELECT query with newlines
    operation_name = " ".join(line.strip() for line in event.job.name.splitlines()).strip()
    # remove parent job name from operation name
    if operation_name.startswith(run.job.name):
        prefix = len(run.job.name) + 1
        operation_name = operation_name[prefix:]

    operation = OperationDTO(
        id=event.run.runId,  # type: ignore [arg-type]
        run=run,
        name=operation_name,
        type=OperationTypeDTO(event.job.facets.jobType.processingType) if event.job.facets.jobType else None,
    )
    enrich_operation_status(operation, event)
    enrich_operation_description(operation, event)
    return operation


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


def enrich_operation_description(operation: OperationDTO, event: OpenLineageRunEvent) -> OperationDTO:
    spark_job_details = event.run.facets.spark_jobDetails
    if spark_job_details:
        operation.position = spark_job_details.jobId
        operation.group = spark_job_details.jobGroup
        operation.description = spark_job_details.jobDescription
    return operation
