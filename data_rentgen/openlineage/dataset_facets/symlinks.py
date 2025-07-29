# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)


class OpenLineageSymlinkType(str, Enum):
    """Dataset symlink type.
    See [SymlinksDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SymlinksDatasetFacet.json).
    """

    TABLE = "TABLE"
    LOCATION = "LOCATION"


class OpenLineageSymlinkIdentifier(OpenLineageBase):
    """Dataset symlink information.
    See [SymlinksDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SymlinksDatasetFacet.json).
    """

    namespace: str
    name: str
    type: OpenLineageSymlinkType


class OpenLineageSymlinksDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing symlinks.
    See [SymlinksDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SymlinksDatasetFacet.json).
    """

    identifiers: list[OpenLineageSymlinkIdentifier]
