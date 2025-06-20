from __future__ import annotations

from datetime import datetime, timezone

from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import DbtExtractor
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
from data_rentgen.consumer.openlineage.run_facets import OpenLineageDbtRunRunFacet, OpenLineageRunFacets
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

    assert DbtExtractor().extract_run(run) == RunDTO(
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


def test_extractors_extract_run_job_openlineage_1_34_plus():
    # https://github.com/OpenLineage/OpenLineage/pull/3738
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
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(
                    invocation_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_run(run) == RunDTO(
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
        external_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
