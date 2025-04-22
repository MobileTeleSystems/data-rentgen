from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.batch_extractor import BatchExtractor
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
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
    SchemaDTO,
    UserDTO,
)


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
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.APPLICATION,
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
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.APPLICATION,
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        inputs=[
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
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable",
                facets=OpenLineageDatasetFacets(
                    lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.CREATE,
                    ),
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
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
            ),
        ],
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        inputs=[
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
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable",
                facets=OpenLineageDatasetFacets(
                    lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.CREATE,
                    ),
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
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
                        rowCount=1_000,
                        size=10 * 1024 * 1024,
                    ),
                ),
            ),
        ],
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        inputs=[
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
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable",
                facets=OpenLineageDatasetFacets(
                    lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.CREATE,
                    ),
                    symlinks=OpenLineageSymlinksDatasetFacet(
                        identifiers=[
                            OpenLineageSymlinkIdentifier(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
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
        ],
    )


@pytest.mark.parametrize(
    "input_transformation",
    [
        # receiving data out of order does not change result
        pytest.param(
            list,
            id="preserve order",
        ),
        pytest.param(
            reversed,
            id="reverse order",
        ),
    ],
)
def test_extractors_extract_batch_spark(
    spark_app_run_event_start: OpenLineageRunEvent,
    spark_app_run_event_stop: OpenLineageRunEvent,
    spark_operation_run_event_start: OpenLineageRunEvent,
    spark_operation_run_event_running: OpenLineageRunEvent,
    spark_operation_run_event_stop: OpenLineageRunEvent,
    extracted_postgres_location: LocationDTO,
    extracted_hdfs_location: LocationDTO,
    extracted_hive_location: LocationDTO,
    extracted_spark_location: LocationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
    extracted_hdfs_dataset_symlink: DatasetSymlinkDTO,
    extracted_hive_dataset_symlink: DatasetSymlinkDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
    extracted_spark_app_run: RunDTO,
    extracted_spark_operation: OperationDTO,
    extracted_postgres_input: InputDTO,
    extracted_hive_output: OutputDTO,
    input_transformation,
):
    events = [
        spark_app_run_event_start,
        spark_operation_run_event_start,
        spark_operation_run_event_running,
        spark_operation_run_event_stop,
        spark_app_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_spark_location,
        extracted_postgres_location,
        extracted_hive_location,
        extracted_hdfs_location,
    ]

    assert extracted.jobs() == [extracted_spark_app_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_spark_app_run]
    assert extracted.operations() == [extracted_spark_operation]

    assert extracted.datasets() == [
        extracted_postgres_dataset,
        extracted_hive_dataset,
        extracted_hdfs_dataset,
    ]

    assert extracted.dataset_symlinks() == [
        extracted_hdfs_dataset_symlink,
        extracted_hive_dataset_symlink,
    ]

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_postgres_input]
    assert extracted.outputs() == [extracted_hive_output]


def test_extractors_extract_batch_spark_strip_hdfs_partitions(extracted_hdfs_dataset: DatasetDTO):
    """
    There is two datasets name in event. They should be union into one, excluding partition.
    """
    event_time = datetime(2024, 7, 5, 9, 7, 9, 849000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    event = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
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
                        namespace="local://some.host.com",
                        name="mysession",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        outputs=[
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable/business_dt=2025-01-01/reg_id=99/part_dt=2025-01-01",
            ),
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable/business_dt=2025-02-01/reg_id=99/part_dt=2025-01-01",
            ),
        ],
    )

    extracted = BatchExtractor().add_events([event])
    assert extracted.datasets() == [extracted_hdfs_dataset]
