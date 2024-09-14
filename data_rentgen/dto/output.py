# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OutputTypeDTO(str, Enum):
    CREATE = "CREATE"
    ALTER = "ALTER"
    RENAME = "RENAME"

    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"

    DROP = "DROP"
    TRUNCATE = "TRUNCATE"

    def __str__(self) -> str:
        return self.value


@dataclass(slots=True)
class OutputDTO:
    type: OutputTypeDTO
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None
