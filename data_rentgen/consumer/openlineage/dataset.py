# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from msgspec import field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageInputDatasetFacets,
    OpenLineageOutputDatasetFacets,
)


class OpenLineageDataset(OpenLineageBase):
    """Generic dataset model.
    See [Dataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    namespace: str
    name: str
    facets: OpenLineageDatasetFacets = field(default_factory=OpenLineageDatasetFacets)


class OpenLineageInputDataset(OpenLineageDataset):
    """Input dataset model.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    inputFacets: OpenLineageInputDatasetFacets = field(default_factory=OpenLineageInputDatasetFacets)


class OpenLineageOutputDataset(OpenLineageDataset):
    """Output dataset model.
    See [OutputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputFacets: OpenLineageOutputDatasetFacets = field(default_factory=OpenLineageOutputDatasetFacets)
