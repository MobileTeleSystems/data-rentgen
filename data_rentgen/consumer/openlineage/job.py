# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from msgspec import field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.job_facets import OpenLineageJobFacets


class OpenLineageJob(OpenLineageBase):
    """Job model.
    See [Job](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    namespace: str
    name: str
    facets: OpenLineageJobFacets = field(default_factory=OpenLineageJobFacets)
