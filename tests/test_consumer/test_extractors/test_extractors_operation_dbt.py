from __future__ import annotations

from datetime import datetime, timezone

from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import DbtExtractor
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
)
from data_rentgen.dto import (
    JobDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    RunStatusDTO,
    SQLQueryDTO,
)


def test_extractors_extract_operation_model():
    now = datetime(2025, 5, 20, 8, 27, 18, 581235, tzinfo=timezone.utc)
    run_id = UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    operation_id = UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=OpenLineageSqlJobFacet(
                    query=(
                        "\n"
                        "\nselect"
                        "\nid,"
                        "\ncomplex_id,"
                        "\n2023 as year,"
                        "\n10 as month,"
                        "\nid as day"
                        "\nfrom demo_schema.source_table"
                    ),
                ),
                # unknown facets are ignored
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        name="dbt-run-demo_project",
                        namespace="local://somehost",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_operation(run) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="dbt-run-demo_project",
                location=LocationDTO(
                    type="local",
                    name="somehost",
                    addresses={"local://somehost"},
                ),
                type=None,
            ),
            status=RunStatusDTO.UNKNOWN,
            started_at=None,
            start_reason=None,
            user=None,
            ended_at=None,
            external_id=None,
            attempt=None,
            persistent_log_url=None,
            running_log_url=None,
        ),
        name="demo_schema.demo_project.target_table",
        type=OperationTypeDTO.BATCH,
        description=None,
        group="MODEL",
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        sql_query=SQLQueryDTO(
            query="select\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
        ),
    )
