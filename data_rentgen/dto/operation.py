# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from uuid6 import UUID

from data_rentgen.dto.run import RunDTO


class OperationTypeDTO(str, Enum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"

    def __str__(self) -> str:
        return str(self.value)


class OperationStatusDTO(str, Enum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    KILLED = "KILLED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)


@dataclass(slots=True)
class OperationDTO:
    id: UUID
    name: str
    run: RunDTO
    type: OperationTypeDTO | None = None
    position: int | None = None
    group: str | None = None
    description: str | None = None
    status: OperationStatusDTO | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
