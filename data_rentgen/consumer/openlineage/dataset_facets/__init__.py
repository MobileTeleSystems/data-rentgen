# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TypedDict

from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.dataquality_metrics import (
    OpenLineageDataQualityMetricsInputDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.datasource import (
    OpenLineageDatasourceDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.documentation import (
    OpenLineageDocumentationDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.lifecycle_change import (
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageDatasetPreviousIdentifier,
    OpenLineageLifecycleStateChangeDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.output_statistics import (
    OpenLineageOutputStatisticsOutputDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.schema import (
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.consumer.openlineage.dataset_facets.storage import (
    OpenLineageStorageDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.symlinks import (
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)

__all__ = [
    "OpenLineageDatasetFacet",
    "OpenLineageDataQualityMetricsInputDatasetFacet",
    "OpenLineageDatasourceDatasetFacet",
    "OpenLineageDocumentationDatasetFacet",
    "OpenLineageLifecycleStateChangeDatasetFacet",
    "OpenLineageDatasetPreviousIdentifier",
    "OpenLineageDatasetLifecycleStateChange",
    "OpenLineageOutputStatisticsOutputDatasetFacet",
    "OpenLineageSchemaDatasetFacet",
    "OpenLineageSchemaField",
    "OpenLineageStorageDatasetFacet",
    "OpenLineageSymlinksDatasetFacet",
    "OpenLineageSymlinkType",
    "OpenLineageSymlinkIdentifier",
    "OpenLineageDatasetFacetsDict",
    "OpenLineageInputDatasetFacetsDict",
    "OpenLineageOutputDatasetFacetsDict",
]


class OpenLineageDatasetFacetsDict(TypedDict, total=False):
    """All possible dataset facets.
    See [Dataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    documentation: OpenLineageDocumentationDatasetFacet
    dataQualityMetrics: OpenLineageDataQualityMetricsInputDatasetFacet
    dataSource: OpenLineageDatasourceDatasetFacet
    lifecycleStateChange: OpenLineageLifecycleStateChangeDatasetFacet
    outputStatistics: OpenLineageOutputStatisticsOutputDatasetFacet
    schema: OpenLineageSchemaDatasetFacet
    storage: OpenLineageStorageDatasetFacet
    symlinks: OpenLineageSymlinksDatasetFacet


class OpenLineageInputDatasetFacetsDict(TypedDict, total=False):
    """All possible input dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    dataQualityMetrics: OpenLineageDataQualityMetricsInputDatasetFacet


class OpenLineageOutputDatasetFacetsDict(TypedDict, total=False):
    """All possible output dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputStatistics: OpenLineageOutputStatisticsOutputDatasetFacet
