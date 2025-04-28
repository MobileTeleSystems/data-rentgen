import pytest

from data_rentgen.consumer.extractors import BatchExtractor
from data_rentgen.consumer.openlineage.run_event import (
    OpenLineageRunEvent,
)
from data_rentgen.dto import (
    JobDTO,
    LocationDTO,
    RunDTO,
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
def test_extractors_extract_batch_airflow(
    airflow_dag_run_event_start: OpenLineageRunEvent,
    airflow_dag_run_event_stop: OpenLineageRunEvent,
    airflow_task_run_event_start: OpenLineageRunEvent,
    airflow_task_run_event_stop: OpenLineageRunEvent,
    extracted_airflow_location: LocationDTO,
    extracted_airflow_dag_job: JobDTO,
    extracted_airflow_task_job: JobDTO,
    extracted_airflow_dag_run: RunDTO,
    extracted_airflow_task_run: RunDTO,
    input_transformation,
):
    events = [
        airflow_dag_run_event_start,
        airflow_task_run_event_start,
        airflow_task_run_event_stop,
        airflow_dag_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [extracted_airflow_location]
    assert extracted.jobs() == [extracted_airflow_dag_job, extracted_airflow_task_job]
    assert extracted.runs() == [
        extracted_airflow_dag_run,
        extracted_airflow_task_run,
    ]

    assert not extracted.datasets()
    assert not extracted.dataset_symlinks()
    assert not extracted.schemas()
    assert not extracted.operations()
    assert not extracted.inputs()
    assert not extracted.outputs()
