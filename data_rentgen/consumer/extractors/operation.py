# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.uuid import extract_created_at_from_uuid
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.dto import OperationDTO, OperationStatusDTO, OperationTypeDTO


def extract_operation(event: OpenLineageRunEvent) -> OperationDTO:
    operation = OperationDTO(
        created_at=extract_created_at_from_uuid(event.run.runId),
        id=event.run.runId,
        name=event.job.name,
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
        operation.description = (
            spark_job_details.jobDescription or spark_job_details.jobGroup or spark_job_details.jobCallSite
        )
    return operation
