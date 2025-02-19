# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.column_lineage import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
)
from data_rentgen.consumer.openlineage.dataset_facets.datasource import (
    OpenLineageDatasourceDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.documentation import (
    OpenLineageDocumentationDatasetFacet,
)
from data_rentgen.consumer.openlineage.dataset_facets.input_statistics import (
    OpenLineageInputStatisticsInputDatasetFacet,
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
    "OpenLineageDatasourceDatasetFacet",
    "OpenLineageDocumentationDatasetFacet",
    "OpenLineageLifecycleStateChangeDatasetFacet",
    "OpenLineageDatasetPreviousIdentifier",
    "OpenLineageDatasetLifecycleStateChange",
    "OpenLineageInputStatisticsInputDatasetFacet",
    "OpenLineageOutputStatisticsOutputDatasetFacet",
    "OpenLineageSchemaDatasetFacet",
    "OpenLineageSchemaField",
    "OpenLineageStorageDatasetFacet",
    "OpenLineageSymlinksDatasetFacet",
    "OpenLineageSymlinkType",
    "OpenLineageSymlinkIdentifier",
    "OpenLineageDatasetFacets",
    "OpenLineageInputDatasetFacets",
    "OpenLineageOutputDatasetFacets",
    "OpenLineageColumnLineageDatasetFacet",
    "OpenLineageColumnLineageDatasetFacetField",
    "OpenLineageColumnLineageDatasetFacetFieldRef",
    "OpenLineageColumnLineageDatasetFacetFieldTransformation",
]


class OpenLineageDatasetFacets(OpenLineageBase):
    """All possible dataset facets.
    See [Dataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    documentation: OpenLineageDocumentationDatasetFacet | None = None
    dataSource: OpenLineageDatasourceDatasetFacet | None = None
    lifecycleStateChange: OpenLineageLifecycleStateChangeDatasetFacet | None = None
    datasetSchema: OpenLineageSchemaDatasetFacet | None = Field(default=None, alias="schema")
    storage: OpenLineageStorageDatasetFacet | None = None
    symlinks: OpenLineageSymlinksDatasetFacet | None = None
    columnLineage: OpenLineageColumnLineageDatasetFacet | None = None


class OpenLineageInputDatasetFacets(OpenLineageBase):
    """All possible input dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    inputStatistics: OpenLineageInputStatisticsInputDatasetFacet | None = None


class OpenLineageOutputDatasetFacets(OpenLineageBase):
    """All possible output dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputStatistics: OpenLineageOutputStatisticsOutputDatasetFacet | None = None
