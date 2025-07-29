from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
    OpenLineageSqlJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageHiveQueryInfoRunFacet,
    OpenLineageHiveSessionInfoRunFacet,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)


@pytest.fixture
def hive_query_run_event_stop() -> OpenLineageRunEvent:
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=datetime(2025, 6, 18, 13, 32, 10, 30000, tzinfo=timezone.utc),
        job=OpenLineageJob(
            namespace="hive://test-hadoop:10000",
            name="createtable_as_select.mydatabase.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="HIVE",
                    jobType="JOB",
                ),
                sql=OpenLineageSqlJobFacet(
                    query="create table mydatabase.target_table as select * from mydatabase.source_table",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("0197833d-6cec-7609-a80f-8f4e0f8a5b1f"),
            facets=OpenLineageRunFacets(
                hive_query=OpenLineageHiveQueryInfoRunFacet(
                    queryId="hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45",
                    operationName="CREATETABLE_AS_SELECT",
                ),
                hive_session=OpenLineageHiveSessionInfoRunFacet(
                    username="myuser",
                    clientIp="11.22.33.44",
                    sessionId="0ba6765b-3019-4172-b748-63c257158d20",
                    creationTime=datetime(2025, 6, 18, 13, 32, 3, 229000, tzinfo=timezone.utc),
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.1.3"),
                    name="hive",
                    openlineageAdapterVersion=Version("1.34.0"),
                ),
            ),
        ),
    )
