# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.openlineage.base import OpenLineageBase


class OpenLineageDatasetFacet(OpenLineageBase):
    """Base class for all dataset facets.
    See [DatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """


class OpenLineageInputDatasetFacet(OpenLineageDatasetFacet):
    """Base class for input dataset facets.
    See [DatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """


class OpenLineageOutputDatasetFacet(OpenLineageDatasetFacet):
    """Base class for output dataset facets.
    See [DatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """
