# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset_facets import (
    OpenLineageDatasetFacetsDict,
    OpenLineageInputDatasetFacetsDict,
    OpenLineageOutputDatasetFacetsDict,
)


class OpenLineageDataset(OpenLineageBase):
    """Generic dataset model.
    See [Dataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    namespace: str = Field(json_schema_extra={"format": "uri"})
    name: str
    facets: OpenLineageDatasetFacetsDict = Field(default_factory=OpenLineageDatasetFacetsDict)  # type: ignore[arg-type]


class OpenLineageInputDataset(OpenLineageDataset):
    """Input dataset model.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    inputFacets: OpenLineageInputDatasetFacetsDict = Field(default_factory=OpenLineageInputDatasetFacetsDict)  # type: ignore[arg-type]


class OpenLineageOutputDataset(OpenLineageDataset):
    """Output dataset model.
    See [OutputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputFacets: OpenLineageOutputDatasetFacetsDict = Field(default_factory=OpenLineageOutputDatasetFacetsDict)  # type: ignore[arg-type]
