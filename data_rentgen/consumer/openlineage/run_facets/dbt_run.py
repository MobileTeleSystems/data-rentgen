# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageDbtRunRunFacet(OpenLineageRunFacet):
    """Run facet describing DBT run.
    See [DbtRunRunFacet](https://github.com/OpenLineage/OpenLineage/pull/3738).
    """

    invocation_id: str
