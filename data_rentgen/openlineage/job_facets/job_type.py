# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from pydantic import Field

from data_rentgen.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageJobProcessingType(str, Enum):
    """Job processing type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    BATCH = "BATCH"
    STREAMING = "STREAMING"
    NONE = "NONE"


class OpenLineageJobTypeJobFacet(OpenLineageJobFacet):
    """Job facet describing job type.
    See [JobTypeJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    processingType: OpenLineageJobProcessingType
    integration: str = Field(examples=["SPARK"])
    jobType: str | None = Field(default=None, examples=["APPLICATION"])
