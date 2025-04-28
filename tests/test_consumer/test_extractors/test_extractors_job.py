from data_rentgen.consumer.extractors import extract_job
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.dto import JobDTO, JobTypeDTO, LocationDTO


def test_extractors_extract_job_spark_yarn():
    job = OpenLineageJob(
        namespace="yarn://cluster",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="SPARK",
                jobType="APPLICATION",
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="yarn", name="cluster", addresses={"yarn://cluster"}),
        type=JobTypeDTO(type="SPARK_APPLICATION"),
    )


def test_extractors_extract_job_spark_local():
    job = OpenLineageJob(
        namespace="host://some.host.com",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="SPARK",
                jobType="APPLICATION",
            ),
        ),
    )
    assert extract_job(job) == JobDTO(
        name="myjob",
        location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
        type=JobTypeDTO(type="SPARK_APPLICATION"),
    )


def test_extractors_extract_job_airflow_dag():
    job = OpenLineageJob(
        namespace="http://airflow-host:8081",
        name="mydag",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.BATCH,
                integration="AIRFLOW",
                jobType="DAG",
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
        type=JobTypeDTO(type="AIRFLOW_DAG"),
    )


def test_extractors_extract_job_airflow_task():
    job = OpenLineageJob(
        namespace="http://airflow-host:8081",
        name="mydag.mytask",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.BATCH,
                integration="AIRFLOW",
                jobType="TASK",
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
        type=JobTypeDTO(type="AIRFLOW_TASK"),
    )


def test_extractors_extract_job_unknown():
    job1 = OpenLineageJob(
        namespace="something",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.BATCH,
                integration="UNKNOWN",
            ),
        ),
    )
    assert extract_job(job1) == JobDTO(
        name="myjob",
        type=JobTypeDTO(type="UNKNOWN"),
        location=LocationDTO(
            type="unknown",
            name="something",
            addresses={"unknown://something"},
        ),
    )

    job2 = OpenLineageJob(
        namespace="something",
        name="myjob",
        facets=OpenLineageJobFacets(
            jobType=OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.BATCH,
                integration="UNKNOWN",
                jobType="SOMETHING",
            ),
        ),
    )
    assert extract_job(job2) == JobDTO(
        name="myjob",
        type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
        location=LocationDTO(
            type="unknown",
            name="something",
            addresses={"unknown://something"},
        ),
    )


def test_extractors_extract_job_no_job_type():
    job = OpenLineageJob(namespace="something", name="myjob")
    assert extract_job(job) == JobDTO(
        name="myjob",
        type=None,
        location=LocationDTO(
            type="unknown",
            name="something",
            addresses={"unknown://something"},
        ),
    )
