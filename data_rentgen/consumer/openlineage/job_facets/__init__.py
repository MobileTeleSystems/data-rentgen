# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TypedDict

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
    "OpenLineageJobFacet",
    "OpenLineageDocumentationJobFacet",
    "OpenLineageJobTypeJobFacet",
    "OpenLineageJobFacetsDict",
    "OpenLineageJobProcessingType",
    "OpenLineageJobIntegrationType",
    "OpenLineageJobType",
]


class OpenLineageJobFacetsDict(TypedDict, total=False):
    """All possible job facets.
    See [Job](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    documentation: OpenLineageDocumentationJobFacet
    jobType: OpenLineageJobTypeJobFacet
