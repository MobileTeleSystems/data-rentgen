from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.generic import GenericExtractor
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
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    RunStatusDTO,
)


def test_extractors_extract_operation_unknown():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(namespace="anything", name="somejob"),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="parentjob",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
    assert GenericExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="somejob",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
            parent_run=RunDTO(
                id=run_id,
                job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            status=RunStatusDTO.STARTED,
            started_at=now,
            ended_at=None,
        ),
        name="somejob",
        type=OperationTypeDTO.BATCH,
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
def test_extractors_extract_operation_unknown_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="somejob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="UNKNOWN",
                    jobType="SOMETHING",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="parentjob",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )

    ended_at = now if expected_status != OperationStatusDTO.UNKNOWN else None

    assert GenericExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="somejob",
                type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
            parent_run=RunDTO(
                id=run_id,
                job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            status=RunStatusDTO(expected_status),
            started_at=None,
            ended_at=ended_at,
        ),
        name="somejob",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )
