# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageHiveSessionInfoRunFacet(OpenLineageRunFacet):
    """Run facet describing Hive session.
    See [HiveSessionInfoFacet](https://github.com/OpenLineage/OpenLineage/blob/main/integration/hive/hive-openlineage-hook/src/main/java/io/openlineage/hive/facets/HiveSessionInfoFacet.java).
    """

    username: str = Field(examples=["myuser"])
    clientIp: str = Field(examples=["11.22.33.44"])
    sessionId: str = Field(examples=["0ba6765b-3019-4172-b748-63c257158d20"])
    creationTime: datetime
