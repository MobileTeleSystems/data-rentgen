# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

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

    namespace: str = Field(json_schema_extra={"format": "uri"})
    name: str
    facets: OpenLineageDatasetFacets = Field(default_factory=OpenLineageDatasetFacets)


class OpenLineageInputDataset(OpenLineageDataset):
    """Input dataset model.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    inputFacets: OpenLineageInputDatasetFacets = Field(default_factory=OpenLineageInputDatasetFacets)


class OpenLineageOutputDataset(OpenLineageDataset):
    """Output dataset model.
    See [OutputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputFacets: OpenLineageOutputDatasetFacets = Field(default_factory=OpenLineageOutputDatasetFacets)
