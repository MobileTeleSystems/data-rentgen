# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.job_facets.base import OpenLineageJobFacet
from data_rentgen.consumer.openlineage.job_facets.documentation import (
    OpenLineageDocumentationJobFacet,
)
from data_rentgen.consumer.openlineage.job_facets.job_type import (
    OpenLineageJobIntegrationType,
    OpenLineageJobProcessingType,
    OpenLineageJobType,
    OpenLineageJobTypeJobFacet,
)

__all__ = [
    "OpenLineageDocumentationJobFacet",
    "OpenLineageJobFacet",
    "OpenLineageJobFacets",
    "OpenLineageJobIntegrationType",
    "OpenLineageJobProcessingType",
    "OpenLineageJobType",
    "OpenLineageJobTypeJobFacet",
]


class OpenLineageJobFacets(OpenLineageBase):
    """All possible job facets.
    See [Job](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    documentation: OpenLineageDocumentationJobFacet | None = None
    jobType: OpenLineageJobTypeJobFacet | None = None
