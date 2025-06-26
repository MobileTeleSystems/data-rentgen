# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum
from functools import cached_property
from uuid import UUID

from data_rentgen.dto.run import RunDTO
from data_rentgen.dto.sql_query import SQLQueryDTO
from data_rentgen.utils.uuid import extract_timestamp_from_uuid


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
    name: str | None = None
    type: OperationTypeDTO | None = None
    position: int | None = None
    group: str | None = None
    description: str | None = None
    status: OperationStatusDTO = OperationStatusDTO.UNKNOWN
    sql_query: SQLQueryDTO | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None

    @property
    def unique_key(self) -> tuple:
        return (self.id,)

    @cached_property
    def created_at(self) -> datetime:
        return extract_timestamp_from_uuid(self.id)

    def merge(self, new: OperationDTO) -> OperationDTO:
        sql_query: SQLQueryDTO | None
        if self.sql_query and new.sql_query:
            sql_query = self.sql_query.merge(new.sql_query)
        else:
            sql_query = new.sql_query or self.sql_query

        return OperationDTO(
            id=self.id,
            run=self.run.merge(new.run),
            name=new.name or self.name,
            type=new.type,
            group=new.group or self.group,
            description=new.description or self.description,
            status=max(new.status, self.status),
            sql_query=sql_query,
            position=new.position or self.position,
            started_at=new.started_at or self.started_at,
            ended_at=new.ended_at or self.ended_at,
        )
