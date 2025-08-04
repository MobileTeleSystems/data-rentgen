# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import UUID7, Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageParentJob(OpenLineageBase):
    """Parent job identifier.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    namespace: str = Field(examples=["http://my-airflow.domain.com:8081"], json_schema_extra={"format": "uri"})
    name: str = Field(examples=["my_dag.my_task"])


class OpenLineageParentRun(OpenLineageBase):
    """Parent run identifier.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    runId: UUID7 = Field(examples=["019867d4-1b59-71fe-bc30-3fbd38703700"])


class OpenLineageParentRoot(OpenLineageBase):
    """Root run started the chain of runs, e.g. Airflow DAG -> Airflow Task -> Spark application -> Spark command.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    job: OpenLineageParentJob
    run: OpenLineageParentRun


class OpenLineageParentRunFacet(OpenLineageRunFacet):
    """Run facet describing parent run.
    See [ParentRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ParentRunFacet.json).
    """

    job: OpenLineageParentJob
    run: OpenLineageParentRun
    root: OpenLineageParentRoot | None = None
