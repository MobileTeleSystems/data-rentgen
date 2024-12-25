# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageDocumentationJobFacet(OpenLineageJobFacet):
    """Job facet describing documentation.
    See [DocumentationJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DocumentationJobFacet.json).
    """

    description: str
