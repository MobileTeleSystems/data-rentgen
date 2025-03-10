# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum
from typing import Annotated

from msgspec import Meta, field

from data_rentgen.consumer.openlineage.base import OpenLineageBase
from data_rentgen.consumer.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.run import OpenLineageRun


class OpenLineageRunEventType(str, Enum):
    """Supported values of run eventType.
    See [RunEvent](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"

    def __str__(self) -> str:
        return self.value


class OpenLineageRunEvent(OpenLineageBase):
    """RunEvent model.
    See [RunEvent](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    eventTime: Annotated[datetime, Meta(tz=True)]
    eventType: OpenLineageRunEventType
    job: OpenLineageJob
    run: OpenLineageRun
    inputs: list[OpenLineageInputDataset] = field(default_factory=list)
    outputs: list[OpenLineageOutputDataset] = field(default_factory=list)
    # ignore producer and schemaURL
