from datetime import datetime, timezone

from data_rentgen.consumer.extractors import extract_run_user
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
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
    OpenLineageRunFacets,
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.db.utils.uuid import generate_new_uuid
from data_rentgen.dto import UserDTO


def test_extractors_extract_user_spark_app():
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=datetime.now(),
        job=OpenLineageJob(
            namespace="yarn://cluster",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=None,
                    integration=OpenLineageJobIntegrationType.SPARK,
                    jobType=OpenLineageJobType.APPLICATION,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=generate_new_uuid(),
            facets=OpenLineageRunFacets(
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="yarn",
                    appName="myapp",
                    applicationId="application_1234_5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040",
                    proxyUrl="http://yarn-proxy:8088/proxy/application_1234_5678",
                    historyUrl="http://history-server:18081/history/application_1234_5678/",
                ),
            ),
        ),
    )
    assert extract_run_user(run) == UserDTO(name="myuser")


def test_extractors_extract_run_airflow_dag():
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=datetime.now(),
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.DAG,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=generate_new_uuid(),
            facets=OpenLineageRunFacets(
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__123",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )

    assert extract_run_user(run) is None


def test_extractors_extract_run_airflow_task():
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=datetime.now(),
        job=OpenLineageJob(
            namespace="airflow://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration=OpenLineageJobIntegrationType.AIRFLOW,
                    jobType=OpenLineageJobType.TASK,
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=generate_new_uuid(),
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__123",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert extract_run_user(run) is None
