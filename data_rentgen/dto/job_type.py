# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class JobTypeDTO:
    type: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.type,)

    def merge(self, new: JobTypeDTO) -> JobTypeDTO:
        if new.id is None and self.type == new.type:
            # job types aren't changed that much, reuse them if possible
            return self

        return JobTypeDTO(
            type=new.type,
            id=new.id or self.id,
        )
