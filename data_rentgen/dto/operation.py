# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum
from uuid import UUID

from data_rentgen.dto.run import RunDTO


class OperationTypeDTO(str, Enum):
    BATCH = "BATCH"
    STREAMING = "STREAMING"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def _missing_(cls, value: object) -> OperationTypeDTO:
        if value == "NONE":
            return OperationTypeDTO.BATCH
        return super()._missing_(value)


class OperationStatusDTO(IntEnum):
    UNKNOWN = -1
    STARTED = 0
    SUCCEEDED = 1
    FAILED = 2
    KILLED = 3


@dataclass
class OperationDTO:
    id: UUID
    run: RunDTO
    name: str
    type: OperationTypeDTO = OperationTypeDTO.BATCH
    position: int | None = None
    group: str | None = None
    description: str | None = None
    status: OperationStatusDTO = OperationStatusDTO.UNKNOWN
    started_at: datetime | None = None
    ended_at: datetime | None = None

    @property
    def unique_key(self) -> tuple:
        return (self.id,)

    def merge(self, new: OperationDTO) -> OperationDTO:
        return OperationDTO(
            id=self.id,
            run=self.run.merge(new.run),
            name=new.name or self.name,
            type=new.type,
            group=new.group or self.group,
            description=new.description or self.description,
            status=max(new.status, self.status),
            position=new.position or self.position,
            started_at=new.started_at or self.started_at,
            ended_at=new.ended_at or self.ended_at,
        )
