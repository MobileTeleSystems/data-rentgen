from __future__ import annotations

from datetime import datetime, timezone

from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import SparkExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
    UserDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)


def test_extractors_extract_run_spark_app_yarn():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="yarn://cluster",
            name="myjob",
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
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                    openlineageAdapterVersion=Version("1.19.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="yarn",
                    appName="myapp",
                    applicationId="application_1234_5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040",
                    proxyUrl="http://yarn-proxy:8088/proxy/application_1234_5678,http://yarn-proxy:18088/proxy/application_1234_5678",
                    historyUrl="http://history-server:18080/history/application_1234_5678,http://history-server:18081/history/application_1234_5678",
                ),
            ),
        ),
    )
    assert SparkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="yarn", name="cluster", addresses={"yarn://cluster"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.19.0",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        start_reason=None,
        user=UserDTO(name="myuser"),
        ended_at=None,
        external_id="application_1234_5678",
        attempt=None,
        persistent_log_url="http://history-server:18080/history/application_1234_5678",
        running_log_url="http://yarn-proxy:8088/proxy/application_1234_5678",
    )


def test_extractors_extract_run_spark_app_local():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="host://some.host.com",
            name="myjob",
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
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                    openlineageAdapterVersion=Version("1.19.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[4]",
                    appName="myapp",
                    applicationId="local-1234-5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040,http://localhost:4041",
                ),
            ),
        ),
    )

    assert SparkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.19.0",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=None,
        start_reason=None,
        user=UserDTO(name="myuser"),
        external_id="local-1234-5678",
        attempt=None,
        persistent_log_url=None,
        running_log_url="http://localhost:4040",
    )
