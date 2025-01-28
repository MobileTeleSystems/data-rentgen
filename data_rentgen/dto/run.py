# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum

from uuid6 import UUID

from data_rentgen.dto.job import JobDTO
from data_rentgen.dto.user import UserDTO


class RunStatusDTO(IntEnum):
    UNKNOWN = -1
    STARTED = 0
    SUCCEEDED = 1
    FAILED = 2
    KILLED = 3


class RunStartReasonDTO(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"

    def __str__(self) -> str:
        return str(self.value)


@dataclass
class RunDTO:
    id: UUID
    job: JobDTO
    parent_run: RunDTO | None = None
    status: RunStatusDTO = RunStatusDTO.UNKNOWN
    started_at: datetime | None = None
    start_reason: RunStartReasonDTO | None = None
    user: UserDTO | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    attempt: str | None = None
    running_log_url: str | None = None
    persistent_log_url: str | None = None

    @property
    def unique_key(self) -> tuple:
        return (self.id,)

    def merge(self, new: RunDTO) -> RunDTO:
        parent_run: RunDTO | None
        if new.parent_run and self.parent_run:
            parent_run = self.parent_run.merge(new.parent_run)
        else:
            parent_run = new.parent_run or self.parent_run

        user: UserDTO | None
        if new.user and self.user:
            user = self.user.merge(new.user)
        else:
            user = new.user or self.user

        return RunDTO(
            id=self.id,
            job=self.job.merge(new.job),
            parent_run=parent_run,
            status=max(new.status, self.status),
            started_at=new.started_at or self.started_at,
            start_reason=new.start_reason or self.start_reason,
            user=user,
            ended_at=new.ended_at or self.ended_at,
            external_id=new.external_id or self.external_id,
            attempt=new.attempt or self.attempt,
            running_log_url=new.running_log_url or self.running_log_url,
            persistent_log_url=new.persistent_log_url or self.persistent_log_url,
        )
