from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import extract_batch, extract_dataset_and_symlinks
from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageStorageDatasetFacet,
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
)
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
)


def test_extractors_extract_dataset_hdfs():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/user/hive/warehouse/mydb.db/mytable",
    )
    assert symlinks == []


def test_extractors_extract_dataset_hdfs_with_patition():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable/business_dt=2025-01-01/reg_id=99/part_dt=2025-01-01",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/user/hive/warehouse/mydb.db/mytable",
    )
    assert symlinks == []


def test_extractors_extract_batch_dataset_hdfs():
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

    extracted = extract_batch([event])
    assert extracted.datasets() == [
        DatasetDTO(
            location=LocationDTO(type="hdfs", name="test-hadoop:9820", addresses={"hdfs://test-hadoop:9820"}, id=None),
            name="/user/hive/warehouse/mydb.db/mytable",
            format=None,
            id=None,
        ),
    ]


def test_extractors_extract_dataset_hdfs_with_table_symlink():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable",
                        type=OpenLineageSymlinkType.TABLE,
                    ),
                ],
            ),
        ),
    )

    hdfs_dataset = DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/warehouse/mydb.db/mytable",
    )

    hive_dataset = DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == hive_dataset
    assert symlinks == [
        DatasetSymlinkDTO(from_dataset=hdfs_dataset, to_dataset=hive_dataset, type=DatasetSymlinkTypeDTO.METASTORE),
        DatasetSymlinkDTO(from_dataset=hive_dataset, to_dataset=hdfs_dataset, type=DatasetSymlinkTypeDTO.WAREHOUSE),
    ]


@pytest.mark.parametrize(
    ["storage_layer", "file_format", "expected_format"],
    [
        ("default", "parquet", "parquet"),
        ("iceberg", "parquet", "iceberg"),
        ("iceberg", "", "iceberg"),
        ("delta", "parquet", "delta"),
    ],
)
def test_extractors_extract_dataset_hdfs_with_format(storage_layer: str, file_format: str, expected_format: str):
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            storage=OpenLineageStorageDatasetFacet(
                storageLayer=storage_layer,
                fileFormat=file_format,
            ),
        ),
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/user/hive/warehouse/mydb.db/mytable",
        format=expected_format,
    )
    assert symlinks == []


def test_extractors_extract_dataset_s3():
    dataset = OpenLineageDataset(
        namespace="s3://bucket",
        name="warehouse/mydb.db/mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="s3",
            name="bucket",
            addresses={"s3://bucket"},
        ),
        name="warehouse/mydb.db/mytable",
    )
    assert symlinks == []


def test_extractors_extract_dataset_file():
    dataset = OpenLineageDataset(
        namespace="file",
        name="/warehouse/mydb.db/mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="file",
            name="unknown",
            addresses={"file://unknown"},
        ),
        name="/warehouse/mydb.db/mytable",
    )
    assert symlinks == []


def test_extractors_extract_dataset_hive():
    dataset = OpenLineageDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )
    assert symlinks == []


def test_extractors_extract_dataset_hive_with_location_symlink():
    # Not accepted yet, see https://github.com/OpenLineage/OpenLineage/issues/2718
    dataset = OpenLineageDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hdfs://test-hadoop:9820",
                        name="/warehouse/mydb.db/mytable",
                        type=OpenLineageSymlinkType.LOCATION,
                    ),
                ],
            ),
        ),
    )

    hdfs_dataset = DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/warehouse/mydb.db/mytable",
    )
    hive_dataset = DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == hive_dataset
    assert symlinks == [
        DatasetSymlinkDTO(from_dataset=hdfs_dataset, to_dataset=hive_dataset, type=DatasetSymlinkTypeDTO.METASTORE),
        DatasetSymlinkDTO(from_dataset=hive_dataset, to_dataset=hdfs_dataset, type=DatasetSymlinkTypeDTO.WAREHOUSE),
    ]


def test_extractors_extract_dataset_postgres():
    dataset = OpenLineageDataset(
        namespace="postgres://192.168.1.1:5432",
        name="mydb.myschema.mytable",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="postgres",
            name="192.168.1.1:5432",
            addresses={"postgres://192.168.1.1:5432"},
        ),
        name="mydb.myschema.mytable",
    )
    assert symlinks == []


def test_extractors_extract_dataset_kafka():
    dataset = OpenLineageDataset(
        namespace="kafka://192.168.1.1:9092,192.168.1.2:9092",
        name="mytopic",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="kafka",
            name="192.168.1.1:9092",
            addresses={"kafka://192.168.1.1:9092", "kafka://192.168.1.2:9092"},
        ),
        name="mytopic",
    )
    assert symlinks == []


def test_extractors_extract_dataset_unknown():
    dataset = OpenLineageDataset(
        namespace="some-namespace",
        name="some.name",
    )

    dataset, symlinks = extract_dataset_and_symlinks(dataset)

    assert dataset == DatasetDTO(
        location=LocationDTO(
            type="unknown",
            name="some-namespace",
            addresses={"unknown://some-namespace"},
        ),
        name="some.name",
    )
    assert symlinks == []
