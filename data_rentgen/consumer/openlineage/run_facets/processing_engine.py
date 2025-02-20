# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Annotated

from packaging.version import Version as RawVersion
from pydantic import PlainSerializer, PlainValidator, WithJsonSchema
from typing_extensions import Doc

from data_rentgen.consumer.openlineage.run_facets.base import OpenLineageRunFacet


def validate_version(value) -> RawVersion:
    if not isinstance(value, RawVersion):
        return RawVersion(value)
    return value


Version = Annotated[
    RawVersion,
    PlainValidator(validate_version),
    PlainSerializer(str, return_type=str),
    WithJsonSchema({"type": "string"}),
    Doc("Version"),
]


class OpenLineageProcessingEngineName(str, Enum):
    AIRFLOW = "Airflow"
    SPARK = "spark"

    def __str__(self) -> str:
        return self.value


class OpenLineageProcessingEngineRunFacet(OpenLineageRunFacet):
    """Run facet describing processing engine.
    See [ProcessingEngineRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ProcessingEngineRunFacet.json).
    """

    name: OpenLineageProcessingEngineName
    version: Version
    openlineageAdapterVersion: Version
