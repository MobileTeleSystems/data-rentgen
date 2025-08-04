# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageDbtRunRunFacet(OpenLineageRunFacet):
    """Run facet describing DBT run.
    See [DbtRunRunFacet](https://github.com/OpenLineage/OpenLineage/pull/3738).
    """

    invocation_id: str = Field(examples=["44f7bc13-4538-42c7-a5be-8edb36c39a45"])
