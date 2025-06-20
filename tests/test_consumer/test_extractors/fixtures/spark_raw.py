from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageLifecycleStateChangeDatasetFacet,
    OpenLineageOutputDatasetFacets,
    OpenLineageOutputStatisticsOutputDatasetFacet,
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)
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
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)


@pytest.fixture
def spark_inputs() -> list[OpenLineageInputDataset]:
    return [
        OpenLineageInputDataset(
            namespace="postgres://192.168.1.1:5432",
            name="mydb.myschema.mytable",
            facets=OpenLineageDatasetFacets(
                schema=OpenLineageSchemaDatasetFacet(
                    fields=[
                        OpenLineageSchemaField(
                            name="dt",
                            type="timestamp",
                            description="Business date",
                        ),
                        OpenLineageSchemaField(
                            name="customer_id",
                            type="decimal(20,0)",
                        ),
                        OpenLineageSchemaField(name="total_spent", type="float"),
                        OpenLineageSchemaField(
                            name="phones",
                            type="array",
                            fields=[
                                OpenLineageSchemaField(
                                    name="_element",
                                    type="string",
                                ),
                            ],
                        ),
                        OpenLineageSchemaField(
                            name="address",
                            type="struct",
                            fields=[
                                OpenLineageSchemaField(
                                    name="street",
                                    type="string",
                                ),
                                OpenLineageSchemaField(name="city", type="string"),
                                OpenLineageSchemaField(name="state", type="string"),
                                OpenLineageSchemaField(name="zip", type="string"),
                            ],
                        ),
                    ],
                ),
            ),
        ),
    ]


@pytest.fixture
def spark_outputs() -> list[OpenLineageOutputDataset]:
    return [
        OpenLineageOutputDataset(
            namespace="hdfs://test-hadoop:9820",
            name="/user/hive/warehouse/mydb.db/mytable1",
            facets=OpenLineageDatasetFacets(
                lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                    lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.CREATE,
                ),
                symlinks=OpenLineageSymlinksDatasetFacet(
                    identifiers=[
                        OpenLineageSymlinkIdentifier(
                            namespace="hive://test-hadoop:9083",
                            name="mydb.mytable1",
                            type=OpenLineageSymlinkType.TABLE,
                        ),
                    ],
                ),
                schema=OpenLineageSchemaDatasetFacet(
                    fields=[
                        OpenLineageSchemaField(
                            name="dt",
                            type="timestamp",
                            description="Business date",
                        ),
                        OpenLineageSchemaField(
                            name="customer_id",
                            type="decimal(20,0)",
                        ),
                        OpenLineageSchemaField(name="total_spent", type="float"),
                        OpenLineageSchemaField(
                            name="phones",
                            type="array",
                            fields=[
                                OpenLineageSchemaField(
                                    name="_element",
                                    type="string",
                                ),
                            ],
                        ),
                        OpenLineageSchemaField(
                            name="address",
                            type="struct",
                            fields=[
                                OpenLineageSchemaField(
                                    name="street",
                                    type="string",
                                ),
                                OpenLineageSchemaField(name="city", type="string"),
                                OpenLineageSchemaField(name="state", type="string"),
                                OpenLineageSchemaField(name="zip", type="string"),
                            ],
                        ),
                    ],
                ),
            ),
            outputFacets=OpenLineageOutputDatasetFacets(
                outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                    rowCount=1_000_000,
                    size=1000 * 1024 * 1024,
                ),
            ),
        ),
    ]


@pytest.fixture
def spark_app_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
            name="mysession",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[*]",
                    appName="spark_session",
                    applicationId="local-1719136537510",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="127.0.0.1",
                    userName="myuser",
                    uiWebUrl="http://127.0.0.1:4040",
                ),
            ),
        ),
    )


@pytest.fixture
def spark_app_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
            name="mysession",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
                ),
            ),
        ),
        run=OpenLineageRun(runId=run_id),
    )


@pytest.fixture
def spark_operation_run_event_start() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def spark_operation_run_event_running() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 7, 9, 849000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def spark_operation_run_event_stop() -> OpenLineageRunEvent:
    event_time = datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
    )
