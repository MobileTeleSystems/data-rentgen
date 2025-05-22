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


def test_extractors_extract_run_job():
    now = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
        ),
    )

    assert extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="dbt-run-demo_project",
            location=LocationDTO(
                type="local",
                name="somehost",
                addresses={"local://somehost"},
            ),
            type=JobTypeDTO(type="DBT_JOB"),
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
