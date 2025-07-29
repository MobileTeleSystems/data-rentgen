from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageDatasetFacets,
    OpenLineageDatasourceDatasetFacet,
    OpenLineageOutputDatasetFacets,
    OpenLineageOutputStatisticsOutputDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
    OpenLineageSqlJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageDbtRunRunFacet,
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)


@pytest.fixture
def dbt_model_sql_facet() -> OpenLineageSqlJobFacet:
    return OpenLineageSqlJobFacet(
        query="\n\nselect\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table",
    )


@pytest.fixture
def dbt_inputs() -> list[OpenLineageInputDataset]:
    return [
        OpenLineageInputDataset(
            namespace="spark://localhost:10000",
            name="demo_schema.source_table",
            facets=OpenLineageDatasetFacets(
                schema=OpenLineageSchemaDatasetFacet(
                    fields=[
                        OpenLineageSchemaField(
                            name="id",
                            type="",
                            description="The primary key for this table",
                            fields=[],
                        ),
                    ],
                ),
            ),
        ),
    ]


@pytest.fixture
def dbt_outputs() -> list[OpenLineageOutputDataset]:
    return [
        OpenLineageOutputDataset(
            namespace="spark://localhost:10000",
            name="demo_schema.target_table",
            facets=OpenLineageDatasetFacets(
                dataSource=OpenLineageDatasourceDatasetFacet(
                    name="spark://localhost:10000",
                    uri="spark://localhost:10000",
                ),
                columnLineage=OpenLineageColumnLineageDatasetFacet(
                    fields={
                        "complex_id": OpenLineageColumnLineageDatasetFacetField(
                            inputFields=[
                                OpenLineageColumnLineageDatasetFacetFieldRef(
                                    namespace="spark://localhost:10000",
                                    name="demo_schema.source_table",
                                    field="complex_id",
                                ),
                            ],
                        ),
                        "day": OpenLineageColumnLineageDatasetFacetField(
                            inputFields=[
                                OpenLineageColumnLineageDatasetFacetFieldRef(
                                    namespace="spark://localhost:10000",
                                    name="demo_schema.source_table",
                                    field="id",
                                ),
                            ],
                        ),
                        "id": OpenLineageColumnLineageDatasetFacetField(
                            inputFields=[
                                OpenLineageColumnLineageDatasetFacetFieldRef(
                                    namespace="spark://localhost:10000",
                                    name="demo_schema.source_table",
                                    field="id",
                                ),
                            ],
                        ),
                    },
                ),
            ),
            outputFacets=OpenLineageOutputDatasetFacets(
                outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                    rowCount=2,
                ),
            ),
        ),
    ]


@pytest.fixture
def dbt_job_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2025, 5, 20, 8, 26, 55, 524789, tzinfo=timezone.utc)
    invocation_id = "93c69fcd-10d0-4639-a4f8-95be0da4476b"
    run_id = UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(invocation_id=invocation_id),
            ),
        ),
    )


@pytest.fixture
def dbt_job_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    invocation_id = "93c69fcd-10d0-4639-a4f8-95be0da4476b"
    run_id = UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(invocation_id=invocation_id),
            ),
        ),
    )


@pytest.fixture
def dbt_model_run_event_start(
    dbt_inputs: list[OpenLineageInputDataset],
    dbt_outputs: list[OpenLineageOutputDataset],
    dbt_model_sql_facet: OpenLineageSqlJobFacet,
) -> OpenLineageRunEvent:
    event_time = datetime(2025, 5, 20, 8, 27, 16, 601799, tzinfo=timezone.utc)
    invocation_id = "93c69fcd-10d0-4639-a4f8-95be0da4476b"
    run_id = UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    operation_id = UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=dbt_model_sql_facet,
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="local://somehost",
                        name="dbt-run-demo_project",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
                dbt_run=OpenLineageDbtRunRunFacet(invocation_id=invocation_id),
            ),
        ),
        inputs=dbt_inputs,
        outputs=dbt_outputs,
    )


@pytest.fixture
def dbt_model_run_event_stop(
    dbt_inputs: list[OpenLineageInputDataset],
    dbt_outputs: list[OpenLineageOutputDataset],
    dbt_model_sql_facet: OpenLineageSqlJobFacet,
) -> OpenLineageRunEvent:
    event_time = datetime(2025, 5, 20, 8, 27, 18, 581235, tzinfo=timezone.utc)
    invocation_id = "93c69fcd-10d0-4639-a4f8-95be0da4476b"
    run_id = UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    operation_id = UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="demo_schema.demo_project.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="MODEL",
                ),
                sql=dbt_model_sql_facet,
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="local://somehost",
                        name="dbt-run-demo_project",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
                dbt_run=OpenLineageDbtRunRunFacet(invocation_id=invocation_id),
            ),
        ),
        inputs=dbt_inputs,
        outputs=dbt_outputs,
    )
