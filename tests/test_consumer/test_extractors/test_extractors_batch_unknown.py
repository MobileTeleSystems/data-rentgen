import pytest

from data_rentgen.consumer.extractors.batch_extractor import BatchExtractor
from data_rentgen.consumer.openlineage.dataset import OpenLineageInputDataset, OpenLineageOutputDataset
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
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
def test_extractors_extract_batch_unknown_without_lineage(
    unknown_run_event_start: OpenLineageRunEvent,
    unknown_run_event_running: OpenLineageRunEvent,
    unknown_run_event_stop: OpenLineageRunEvent,
    extracted_unknown_run_location: LocationDTO,
    extracted_unknown_job: JobDTO,
    extracted_unknown_run: RunDTO,
    input_transformation,
):
    events = [
        unknown_run_event_start,
        unknown_run_event_running,
        unknown_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [extracted_unknown_run_location]
    assert extracted.jobs() == [extracted_unknown_job]
    assert extracted.runs() == [extracted_unknown_run]
    assert not extracted.users()

    assert not extracted.operations()
    assert not extracted.datasets()
    assert not extracted.dataset_symlinks()
    assert not extracted.schemas()
    assert not extracted.inputs()
    assert not extracted.outputs()


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
def test_extractors_extract_batch_unknown_with_lineage(
    unknown_run_event_start: OpenLineageRunEvent,
    unknown_run_event_running: OpenLineageRunEvent,
    unknown_run_event_stop: OpenLineageRunEvent,
    postgres_input: OpenLineageInputDataset,
    hdfs_output: OpenLineageOutputDataset,
    hdfs_output_with_stats: OpenLineageOutputDataset,
    extracted_postgres_location: LocationDTO,
    extracted_hdfs_location: LocationDTO,
    extracted_hive_location: LocationDTO,
    extracted_unknown_run_location: LocationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_hdfs_dataset: DatasetDTO,
    extracted_hive_dataset: DatasetDTO,
    extracted_hdfs_dataset_symlink: DatasetSymlinkDTO,
    extracted_hive_dataset_symlink: DatasetSymlinkDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_unknown_job: JobDTO,
    extracted_unknown_run: RunDTO,
    extracted_unknown_operation: OperationDTO,
    extracted_unknown_postgres_input: InputDTO,
    extracted_unknown_hive_output: OutputDTO,
    input_transformation,
):
    events = [
        unknown_run_event_start.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output],
            },
        ),
        unknown_run_event_running.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output_with_stats],
            },
        ),
        unknown_run_event_stop.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output_with_stats],
            },
        ),
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_unknown_run_location,
        extracted_postgres_location,
        extracted_hive_location,
        extracted_hdfs_location,
    ]

    assert extracted.jobs() == [extracted_unknown_job]
    assert extracted.runs() == [extracted_unknown_run]
    assert extracted.operations() == [extracted_unknown_operation]
    assert not extracted.users()

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
    assert extracted.inputs() == [extracted_unknown_postgres_input]
    assert extracted.outputs() == [extracted_unknown_hive_output]
