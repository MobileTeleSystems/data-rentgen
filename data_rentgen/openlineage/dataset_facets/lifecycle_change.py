# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageDatasetLifecycleStateChange(str, Enum):
    """Lifecycle state change type.
    See [LifecycleStateChangeDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/LifecycleStateChangeDatasetFacet.json).
    """

    ALTER = "ALTER"
    CREATE = "CREATE"
    DROP = "DROP"
    OVERWRITE = "OVERWRITE"
    RENAME = "RENAME"
    TRUNCATE = "TRUNCATE"

    def __str__(self) -> str:
        return self.value


class OpenLineageDatasetPreviousIdentifier(OpenLineageBase):
    """Previous identifier information. Used only if `lifecycleStateChange=RENAME`.
    See [LifecycleStateChangeDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/LifecycleStateChangeDatasetFacet.json).
    """

    namespace: str
    name: str


class OpenLineageLifecycleStateChangeDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing lifecycle state change.
    See [LifecycleStateChangeDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/LifecycleStateChangeDatasetFacet.json).
    """

    lifecycleStateChange: OpenLineageDatasetLifecycleStateChange
    previousIdentifier: OpenLineageDatasetPreviousIdentifier | None = None
