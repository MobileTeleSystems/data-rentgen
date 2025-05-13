from __future__ import annotations

from datetime import datetime, timezone

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


def test_extractors_extract_run_flink():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
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
        run=OpenLineageRun(runId=run_id),
    )
    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="flink", name="localhost:18081", addresses={"flink://localhost:18081"}),
            type=JobTypeDTO(type="FLINK_JOB"),
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        start_reason=None,
        user=None,
        ended_at=None,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
