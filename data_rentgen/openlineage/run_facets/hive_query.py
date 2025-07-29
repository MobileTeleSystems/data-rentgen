# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageHiveQueryInfoRunFacet(OpenLineageRunFacet):
    """Run facet describing Hive query.
    See [HiveQueryInfoFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/hive/hive-openlineage-hook/src/main/java/io/openlineage/hive/facets/HiveQueryInfoFacet.java).
    """

    queryId: str
    operationName: str
