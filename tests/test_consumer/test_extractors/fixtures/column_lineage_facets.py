from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
    OpenLineageDatasetFacets,
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


@pytest.fixture
def output_event_with_one_to_two_direct_column_lineage() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="TRANSFORMATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                },
            ),
        ),
    )


@pytest.fixture
def output_event_with_one_to_two_direct_and_indirect_column_lineage() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="JOIN",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="TRANSFORMATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                },
                dataset=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable",
                        field="source_col_2",
                        transformations=[
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="INDIRECT",
                                subtype="SORT",
                                masking=False,
                            ),
                        ],
                    ),
                ],
            ),
        ),
    )


def get_run_event_with_column_lineage(
    operation_id: UUID,
    column_lineage_facet: OpenLineageColumnLineageDatasetFacet,
) -> OpenLineageRunEvent:
    """
    Function for generating run events. One event = one operation.
    Args is: operation_id and column lineage facet, which will be add to outputs.
    Input is always PG dataset, output - hive dataset.
    """
    event_time = datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    return OpenLineageRunEvent(
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
        inputs=[
            OpenLineageInputDataset(
                namespace="postgres://192.168.1.1:5432",
                name="mydb.myschema.mytable",
                facets=OpenLineageDatasetFacets(),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="hive://test-hadoop:9083",
                name="mydb.mytable",
                facets=OpenLineageDatasetFacets(columnLineage=column_lineage_facet),
            ),
        ],
    )
