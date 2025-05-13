from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

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
from data_rentgen.consumer.openlineage.run_facets import (
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)


@pytest.fixture
def flink_job_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-7fc0-9D69-b1df9dac2c60")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://flink-host:18081",
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
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.19.0"),
                    name="FLINK",
                    openlineageAdapterVersion=Version("1.31.0"),
                ),
            ),
        ),
    )


@pytest.fixture
def flink_job_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-7fc0-9D69-b1df9dac2c60")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="http://flink-host:18081",
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
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.19.0"),
                    name="FLINK",
                    openlineageAdapterVersion=Version("1.31.0"),
                ),
            ),
        ),
    )
