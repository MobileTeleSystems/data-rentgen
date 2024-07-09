# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from uuid6 import UUID

from data_rentgen.dto.job import JobDTO


class RunDTOStatus(str, Enum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    KILLED = "KILLED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass
class RunDTO:
    id: UUID
    job: JobDTO
    created_at: datetime
    status: RunDTOStatus | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
