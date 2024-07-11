# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from uuid6 import UUID


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
    created_at: datetime
    id: UUID
    name: str
    type: OperationTypeDTO | None = None
    description: str | None = None
    status: OperationStatusDTO | None = None
    position: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
