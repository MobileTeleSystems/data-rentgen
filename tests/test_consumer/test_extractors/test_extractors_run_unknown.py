from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_run
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
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStatusDTO,
)


@pytest.mark.parametrize(
    ["raw_job_type", "extracted_job_type"],
    [
        (None, None),
        (
            OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="ABC",
            ),
            JobTypeDTO(type="ABC"),
        ),
        (
            OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="ABC",
                jobType="CDE",
            ),
            JobTypeDTO(type="ABC_CDE"),
        ),
    ],
)
def test_extractors_extract_run_unknown(
    raw_job_type: OpenLineageJobTypeJobFacet | None,
    extracted_job_type: JobTypeDTO | None,
):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="something",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=raw_job_type,
            ),
        ),
        run=OpenLineageRun(runId=run_id),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            type=extracted_job_type,
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=now,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.FAIL, RunStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, RunStatusDTO.KILLED),
        (OpenLineageRunEventType.OTHER, RunStatusDTO.UNKNOWN),
    ],
)
def test_extractors_extract_run_with_status(
    event_type: OpenLineageRunEventType,
    expected_status: RunStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(namespace="something", name="myjob"),
        run=OpenLineageRun(runId=run_id),
    )

    ended_at = now if expected_status != RunStatusDTO.UNKNOWN else None
    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
        ),
        status=expected_status,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=ended_at,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
