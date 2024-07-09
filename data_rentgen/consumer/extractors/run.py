# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.job import extract_job
from data_rentgen.consumer.extractors.uuid import extract_created_at_from_uuid
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.consumer.openlineage.run_facets import OpenLineageParentRunFacet
from data_rentgen.dto import RunDTO, RunDTOStatus


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
    return run


def enrich_run_status(run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
    match event.eventType:
        case OpenLineageRunEventType.START:
            run.started_at = event.eventTime
            run.status = RunDTOStatus.STARTED
        case OpenLineageRunEventType.RUNNING:
            run.status = RunDTOStatus.STARTED
        case OpenLineageRunEventType.COMPLETE:
            run.ended_at = event.eventTime
            run.status = RunDTOStatus.SUCCEEDED
        case OpenLineageRunEventType.FAIL:
            run.ended_at = event.eventTime
            run.status = RunDTOStatus.FAILED
        case OpenLineageRunEventType.ABORT:
            run.ended_at = event.eventTime
            run.status = RunDTOStatus.KILLED
    return run
