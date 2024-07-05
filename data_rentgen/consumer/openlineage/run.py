# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.run_facets import OpenLineageRunFacetsDict
from data_rentgen.consumer.openlineage.uuid import UUID


class OpenLineageRun(OpenLineageBase):
    """Run model.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    runId: UUID
    facets: OpenLineageRunFacetsDict = Field(default_factory=OpenLineageRunFacetsDict)  # type: ignore[arg-type]
