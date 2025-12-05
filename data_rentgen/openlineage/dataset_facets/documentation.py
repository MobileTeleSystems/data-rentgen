# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageDocumentationDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing documentation.
    See [DocumentationDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DocumentationDatasetFacet.json).
    """

    description: str
