from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_operation
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
from data_rentgen.dto import OperationDTO, OperationStatusDTO
from data_rentgen.dto.job import JobDTO
from data_rentgen.dto.job_type import JobTypeDTO
from data_rentgen.dto.location import LocationDTO
from data_rentgen.dto.operation import OperationTypeDTO
from data_rentgen.dto.run import RunDTO, RunStatusDTO


def test_extractors_extract_operation_flink_job():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="flink://localhost:18081",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.STREAMING,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                type=JobTypeDTO(type="FLINK_JOB"),
                location=LocationDTO(
                    type="flink",
                    name="localhost:18081",
                    addresses={"flink://localhost:18081"},
                ),
            ),
            status=RunStatusDTO.STARTED,
            started_at=now,
            ended_at=None,
        ),
        name="myjob",
        type=OperationTypeDTO.STREAMING,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.COMPLETE, OperationStatusDTO.SUCCEEDED),
        (OpenLineageRunEventType.FAIL, OperationStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, OperationStatusDTO.KILLED),
        (OpenLineageRunEventType.OTHER, OperationStatusDTO.UNKNOWN),
    ],
)
def test_extractors_extract_operation_flink_job_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(
            namespace="flink://localhost:18081",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
        ),
    )

    ended_at = now if expected_status != OperationStatusDTO.UNKNOWN else None
    assert extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                type=JobTypeDTO(type="FLINK_JOB"),
                location=LocationDTO(
                    type="flink",
                    name="localhost:18081",
                    addresses={"flink://localhost:18081"},
                ),
            ),
            status=RunStatusDTO(expected_status),
            started_at=None,
            ended_at=ended_at,
        ),
        name="myjob",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )
