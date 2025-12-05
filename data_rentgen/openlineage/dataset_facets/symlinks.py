# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

from pydantic import Field

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

    namespace: str = Field(examples=["hdfs://rnd-dwh"], json_schema_extra={"format": "uri"})
    name: str = Field(examples=["/app/warehouse/hive/managed/someschema.db/sometable"])
    type: OpenLineageSymlinkType = Field(examples=["LOCATION"])


class OpenLineageSymlinksDatasetFacet(OpenLineageDatasetFacet):
    """Dataset facet describing symlinks.
    See [SymlinksDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SymlinksDatasetFacet.json).
    """

    identifiers: list[OpenLineageSymlinkIdentifier]
