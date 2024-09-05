# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class InteractionTypeDTO(str, Enum):
    READ = "READ"

    CREATE = "CREATE"
    ALTER = "ALTER"
    RENAME = "RENAME"

    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"

    DROP = "DROP"
    TRUNCATE = "TRUNCATE"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def write_interactions(cls) -> list[InteractionTypeDTO]:
        return [item for item in InteractionTypeDTO if item != InteractionTypeDTO.READ]


@dataclass(slots=True)
class InteractionDTO:
    type: InteractionTypeDTO
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None
