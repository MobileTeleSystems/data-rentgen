# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase


class OpenLineageRunFacet(OpenLineageBase):
    """Base class for all run facets.
    See [RunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """
