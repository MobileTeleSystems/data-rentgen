# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from data_rentgen.consumer.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageSqlJobFacet(OpenLineageJobFacet):
    query: str
