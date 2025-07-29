# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageDatasourceDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing data source information.
    See [DatasourceDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DatasourceDatasetFacet.json).
    """

    name: str
    uri: str
