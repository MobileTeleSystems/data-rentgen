# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageHiveQueryInfoRunFacet(OpenLineageRunFacet):
    """Run facet describing Hive query.
    See [HiveQueryInfoFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/hive/hive-openlineage-hook/src/main/java/io/openlineage/hive/facets/HiveQueryInfoFacet.java).
    """

    queryId: str = Field(examples=["hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45"])
    operationName: str = Field(examples=["CREATETABLE_AS_SELECT"])
