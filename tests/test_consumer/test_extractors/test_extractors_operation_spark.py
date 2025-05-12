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
    OpenLineageSqlJobFacet,
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
from data_rentgen.dto.job import JobDTO
from data_rentgen.dto.location import LocationDTO
from data_rentgen.dto.operation import OperationTypeDTO
from data_rentgen.dto.run import RunDTO
from data_rentgen.dto.sql_query import SQLQueryDTO


def test_extractors_extract_operation_spark_job_no_details():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
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
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
        ),
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
    )


@pytest.mark.parametrize(
    ["job_id", "job_description", "job_group", "job_call_site"],
    [
        pytest.param(1, None, None, None, id="no details"),
        pytest.param(1, "some description", None, None, id="only description"),
        pytest.param(1, "some description", None, "toPandas af file.py:212", id="description with call site"),
        pytest.param(1, None, "some group", None, id="only group"),
        pytest.param(1, "some description", "some group", None, id="description and group"),
        pytest.param(
            1,
            "some description",
            "some group",
            "toPandas af file.py:212",
            id="description, group and call site",
        ),
    ],
)
def test_extractors_extract_operation_spark_job_with_details(
    job_id: int,
    job_description: str | None,
    job_group: str | None,
    job_call_site: str | None,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.STREAMING,
                    integration="SPARK",
                    jobType="SQL_JOB",
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
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
        ),
        name="execute_some_command",
        type=OperationTypeDTO.STREAMING,
        position=job_id,
        group=job_group,
        description=job_description,
        status=OperationStatusDTO.STARTED,
        started_at=None,
        ended_at=None,
    )


def test_extractors_extract_operation_spark_job_name_contains_newlines():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
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
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
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
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
        ),
        # appName prefix is dropped
        name="scan_jdbc_relation((SELECT * FROM some_table) T)",
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
def test_extractors_extract_operation_spark_job_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
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
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )

    ended_at = now if expected_status != OperationStatusDTO.UNKNOWN else None
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
        ),
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )


def test_extractors_extract_operation_spark_job_sql_query():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="mysession.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
                ),
                sql=OpenLineageSqlJobFacet(
                    query="""
                        select id, name from schema.table where id = 1
                    """,
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
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
    assert extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="mysession",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
        ),
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        sql_query=SQLQueryDTO(query="select id, name from schema.table where id = 1"),
        started_at=now,
        ended_at=None,
    )
