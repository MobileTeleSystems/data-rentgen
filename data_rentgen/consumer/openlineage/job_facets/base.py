# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase


class OpenLineageJobFacet(OpenLineageBase):
    """Base class for all job facets.
    See [JobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """
