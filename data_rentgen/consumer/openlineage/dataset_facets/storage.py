# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageStorageDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing storage information.
    See [StorageDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/StorageDatasetFacet.json).
    """

    storageLayer: str
    fileFormat: str
