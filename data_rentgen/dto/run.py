# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from uuid6 import UUID

from data_rentgen.dto.job import JobDTO


class RunStatusDTO(str, Enum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    KILLED = "KILLED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass(slots=True)
class RunDTO:
    created_at: datetime
    id: UUID
    job: JobDTO
    status: RunStatusDTO | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    attempt: str | None = None
    persistent_log_url: str | None = None
    running_log_url: str | None = None
