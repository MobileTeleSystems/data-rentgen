# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from enum import Enum

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.run import OpenLineageRun


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

    eventTime: datetime
    eventType: OpenLineageRunEventType
    job: OpenLineageJob
    run: OpenLineageRun
    inputs: list[OpenLineageInputDataset] = Field(default_factory=list)
    outputs: list[OpenLineageOutputDataset] = Field(default_factory=list)
