# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageHiveSessionInfoRunFacet(OpenLineageRunFacet):
    """Run facet describing Hive session.
    See [HiveSessionInfoFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/hive/hive-openlineage-hook/src/main/java/io/openlineage/hive/facets/HiveSessionInfoFacet.java).
    """

    username: str
    clientIp: str
    sessionId: str
    creationTime: datetime
