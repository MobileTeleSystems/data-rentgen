from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.batch import extract_batch
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
    DatasetSymlinkTypeDTO,
    InputDTO,
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    OutputDTO,
    OutputTypeDTO,
    RunDTO,
    RunStatusDTO,
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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
                            OpenLineageSchemaField(name="customer_id", type="decimal(20,0)"),
                            OpenLineageSchemaField(name="total_spent", type="float"),
                            OpenLineageSchemaField(
                                name="phones",
                                type="array",
                                fields=[
                                    OpenLineageSchemaField(name="_element", type="string"),
                                ],
                            ),
                            OpenLineageSchemaField(
                                name="address",
                                type="struct",
                                fields=[
                                    OpenLineageSchemaField(name="street", type="string"),
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


@pytest.fixture
def extracted_postgres_location() -> LocationDTO:
    return LocationDTO(
        type="postgres",
        name="192.168.1.1:5432",
        addresses={"postgres://192.168.1.1:5432"},
    )


@pytest.fixture
def extracted_hdfs_location() -> LocationDTO:
    return LocationDTO(
        type="hdfs",
        name="test-hadoop:9820",
        addresses={"hdfs://test-hadoop:9820"},
    )


@pytest.fixture
def extracted_hive_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:9083",
        addresses={"hive://test-hadoop:9083"},
    )


@pytest.fixture
def extracted_spark_location() -> LocationDTO:
    return LocationDTO(
        type="local",
        name="some.host.com",
        addresses={"local://some.host.com"},
    )


@pytest.fixture
def extracted_postgres_dataset(
    extracted_postgres_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_postgres_location,
        name="mydb.myschema.mytable",
    )


@pytest.fixture
def extracted_hdfs_dataset(
    extracted_hdfs_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hdfs_location,
        name="/user/hive/warehouse/mydb.db/mytable",
    )


@pytest.fixture
def extracted_hive_dataset(
    extracted_hive_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hive_location,
        name="mydb.mytable",
    )


@pytest.fixture
def extracted_hdfs_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hdfs_dataset,
        to_dataset=extracted_hive_dataset,
        type=DatasetSymlinkTypeDTO.METASTORE,
    )


@pytest.fixture
def extracted_hive_dataset_symlink(
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hive_dataset,
        to_dataset=extracted_hdfs_dataset,
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    )


@pytest.fixture
def extracted_dataset_schema() -> SchemaDTO:
    return SchemaDTO(
        fields=[
            {
                "name": "dt",
                "type": "timestamp",
                "description": "Business date",
            },
            {
                "name": "customer_id",
                "type": "decimal(20,0)",
            },
            {
                "name": "total_spent",
                "type": "float",
            },
            {
                "name": "phones",
                "type": "array",
                "fields": [
                    {
                        "name": "_element",
                        "type": "string",
                    },
                ],
            },
            {
                "name": "address",
                "type": "struct",
                "fields": [
                    {
                        "name": "street",
                        "type": "string",
                    },
                    {
                        "name": "city",
                        "type": "string",
                    },
                    {
                        "name": "state",
                        "type": "string",
                    },
                    {
                        "name": "zip",
                        "type": "string",
                    },
                ],
            },
        ],
    )


@pytest.fixture
def extracted_user() -> UserDTO:
    return UserDTO(name="myuser")


@pytest.fixture
def extracted_spark_app_job(
    extracted_spark_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession",
        location=extracted_spark_location,
        type=JobTypeDTO.SPARK_APPLICATION,
    )


@pytest.fixture
def extracted_spark_app_run(
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        job=extracted_spark_app_job,
        user=extracted_user,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
        external_id="local-1719136537510",
        running_log_url="http://127.0.0.1:4040",
    )


@pytest.fixture
def extracted_spark_operation(
    extracted_spark_app_run: RunDTO,
):
    return OperationDTO(
        id=UUID("01908225-1fd7-746b-910c-70d24f2898b1"),
        name="execute_some_command",
        run=extracted_spark_app_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_postgres_input(
    extracted_spark_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        operation=extracted_spark_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_hdfs_output(
    extracted_spark_operation: OperationDTO,
    extracted_hdfs_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        type=OutputTypeDTO.CREATE,
        operation=extracted_spark_operation,
        dataset=extracted_hdfs_dataset,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
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
    extracted_hdfs_output: OutputDTO,
    input_transformation,
):
    events = [
        spark_app_run_event_start,
        spark_operation_run_event_start,
        spark_operation_run_event_running,
        spark_operation_run_event_stop,
        spark_app_run_event_stop,
    ]

    extracted = extract_batch(input_transformation(events))

    assert extracted.locations() == [
        extracted_spark_location,
        extracted_postgres_location,
        extracted_hdfs_location,
        extracted_hive_location,
    ]

    assert extracted.jobs() == [extracted_spark_app_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_spark_app_run]
    assert extracted.operations() == [extracted_spark_operation]

    assert extracted.datasets() == [
        extracted_postgres_dataset,
        extracted_hdfs_dataset,
        extracted_hive_dataset,
    ]

    assert extracted.dataset_symlinks() == [
        extracted_hdfs_dataset_symlink,
        extracted_hive_dataset_symlink,
    ]

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_postgres_input]
    assert extracted.outputs() == [extracted_hdfs_output]
