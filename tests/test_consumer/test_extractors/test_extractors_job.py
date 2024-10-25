from data_rentgen.consumer.extractors import extract_job
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobIntegrationType,
    OpenLineageJobType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.dto import JobDTO, JobTypeDTO, LocationDTO


def test_extractors_extract_job_spark_yarn():
    job = OpenLineageJob(
        namespace="yarn://cluster",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=None,
                integration=OpenLineageJobIntegrationType.SPARK,
                jobType=OpenLineageJobType.APPLICATION,
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="yarn", name="cluster", addresses={"yarn://cluster"}),
        type=JobTypeDTO.SPARK_APPLICATION,
    )


def test_extractors_extract_job_spark_local():
    job = OpenLineageJob(
        namespace="host://some.host.com",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=None,
                integration=OpenLineageJobIntegrationType.SPARK,
                jobType=OpenLineageJobType.APPLICATION,
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
        type=JobTypeDTO.SPARK_APPLICATION,
    )


def test_extractors_extract_job_airflow_dag():
    job = OpenLineageJob(
        namespace="http://airflow-host:8081",
        name="mydag",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=None,
                integration=OpenLineageJobIntegrationType.AIRFLOW,
                jobType=OpenLineageJobType.DAG,
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="mydag",
        location=LocationDTO(
            type="http",
            name="airflow-host:8081",
            addresses={"http://airflow-host:8081"},
        ),
        type=JobTypeDTO.AIRFLOW_DAG,
    )


def test_extractors_extract_job_airflow_task():
    job = OpenLineageJob(
        namespace="http://airflow-host:8081",
        name="mydag.mytask",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=None,
                integration=OpenLineageJobIntegrationType.AIRFLOW,
                jobType=OpenLineageJobType.TASK,
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="mydag.mytask",
        location=LocationDTO(
            type="http",
            name="airflow-host:8081",
            addresses={"http://airflow-host:8081"},
        ),
        type=JobTypeDTO.AIRFLOW_TASK,
    )


def test_extractors_extract_job_unknown():
    job = OpenLineageJob(namespace="something", name="myjob")
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(
            type="unknown",
            name="something",
            addresses={"unknown://something"},
        ),
    )
