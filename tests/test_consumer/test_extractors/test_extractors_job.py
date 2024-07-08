from data_rentgen.consumer.extractors import extract_job, extract_job_location
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.dto import JobDTO, LocationDTO


def test_extractors_extract_job_spark_yarn():
    job = OpenLineageJob(namespace="yarn://cluster", name="myjob")
    assert extract_job(job) == JobDTO(name="myjob")
    assert extract_job_location(job) == LocationDTO(type="yarn", name="cluster", urls=["yarn://cluster"])


def test_extractors_extract_job_location_spark_local():
    job = OpenLineageJob(namespace="host://some.host.com", name="myjob")
    assert extract_job(job) == JobDTO(name="myjob")
    assert extract_job_location(job) == LocationDTO(type="host", name="some.host.com", urls=["host://some.host.com"])


def test_extractors_extract_job_location_airflow():
    job = OpenLineageJob(namespace="airflow://airflow-host:8081", name="mydag.mytask")
    assert extract_job(job) == JobDTO(name="mydag.mytask")
    assert extract_job_location(job) == LocationDTO(
        type="airflow",
        name="airflow-host:8081",
        urls=["airflow://airflow-host:8081"],
    )


def test_extractors_extract_job_location_unknown():
    job = OpenLineageJob(namespace="something", name="myjob")
    assert extract_job(job) == JobDTO(name="myjob")
    assert extract_job_location(job) == LocationDTO(
        type="unknown",
        name="something",
        urls=["unknown://something"],
    )
