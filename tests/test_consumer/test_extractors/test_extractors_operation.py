from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_operation
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobIntegrationType,
    OpenLineageJobProcessingType,
    OpenLineageJobType,
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
    OpenLineageSparkJobDetailsRunFacet,
)
from data_rentgen.dto import OperationDTO, OperationStatusDTO
from data_rentgen.dto.operation import OperationTypeDTO


def test_extractors_extract_operation_spark_job_no_details():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    jobType=OpenLineageJobType.JOB,
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.SPARK,
                ),
            ),
        ),
        run=OpenLineageRun(runId=operation_id),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        name="mysession.execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
    )


@pytest.mark.parametrize(
    ["job_id", "job_description", "job_group", "job_call_site", "expected_position", "expected_description"],
    [
        (1, None, None, "toPandas af file.py:212", 1, "toPandas af file.py:212"),
        (1, None, "some group", "toPandas af file.py:212", 1, "some group"),
        (1, "some description", "some group", "toPandas af file.py:212", 1, "some description"),
        (1, "some description", "some group", None, 1, "some description"),
        (1, "some description", None, None, 1, "some description"),
    ],
)
def test_extractors_extract_operation_spark_job_with_details(
    job_id: int,
    job_description: str | None,
    job_group: str | None,
    job_call_site: str | None,
    expected_position: int,
    expected_description: str,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    jobType=OpenLineageJobType.JOB,
                    processingType=OpenLineageJobProcessingType.STREAMING,
                    integration=OpenLineageJobIntegrationType.SPARK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                spark_jobDetails=OpenLineageSparkJobDetailsRunFacet(
                    jobId=job_id,
                    jobDescription=job_description,
                    jobGroup=job_group,
                    jobCallSite=job_call_site,
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        name="mysession.execute_some_command",
        type=OperationTypeDTO.STREAMING,
        position=expected_position,
        description=expected_description,
        status=OperationStatusDTO.STARTED,
        started_at=None,
        ended_at=None,
    )


def test_extractors_extract_operation_spark_job_with_parent():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    jobType=OpenLineageJobType.JOB,
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.SPARK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
                    ),
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        # session name prefix is dropped
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
    )


def test_extractors_extract_operation_spark_job_name_contains_newlines():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="""mysession.scan_jdbc_relation((SELECT *
            FROM some_table) T)
            """,
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    jobType=OpenLineageJobType.JOB,
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.SPARK,
                ),
            ),
        ),
        run=OpenLineageRun(runId=operation_id),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        # appName prefix is dropped
        name="mysession.scan_jdbc_relation((SELECT * FROM some_table) T)",
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
        (OpenLineageRunEventType.OTHER, None),
    ],
)
def test_extractors_extract_operation_spark_job_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO | None,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(namespace="anything", name="mysession.execute_some_command"),
        run=OpenLineageRun(runId=operation_id),
    )

    ended_at = now if expected_status else None
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        name="mysession.execute_some_command",
        type=None,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )
