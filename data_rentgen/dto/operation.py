# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from uuid6 import UUID

from data_rentgen.dto.interaction import InteractionTypeDTO


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
    type: OperationTypeDTO | None = None
    description: str | None = None
    status: OperationStatusDTO | None = None
    position: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


@dataclass(slots=True)
class OperationDatasetDTO:
    operation_id: UUID
    dataset_id: int
    interaction_type: InteractionTypeDTO


@dataclass(slots=True)
class OperationNodeDTO:
    id: UUID
    name: str
    status: str
    type: str
