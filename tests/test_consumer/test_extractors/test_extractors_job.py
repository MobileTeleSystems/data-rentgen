from data_rentgen.consumer.extractors import extract_job
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.dto import JobDTO, LocationDTO


def test_extractors_extract_job_spark_yarn():
    job = OpenLineageJob(namespace="yarn://cluster", name="myjob")
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="yarn", name="cluster", addresses=["yarn://cluster"]),
    )


def test_extractors_extract_job_spark_local():
    job = OpenLineageJob(namespace="host://some.host.com", name="myjob")
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="host", name="some.host.com", addresses=["host://some.host.com"]),
    )


def test_extractors_extract_job_airflow():
    job = OpenLineageJob(namespace="http://airflow-host:8081", name="mydag.mytask")
    assert extract_job(job) == JobDTO(
        name="mydag.mytask",
        location=LocationDTO(
            type="http",
            name="airflow-host:8081",
            addresses=["http://airflow-host:8081"],
        ),
    )


def test_extractors_extract_job_unknown():
    job = OpenLineageJob(namespace="something", name="myjob")
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(
            type="unknown",
            name="something",
            addresses=["unknown://something"],
        ),
    )
