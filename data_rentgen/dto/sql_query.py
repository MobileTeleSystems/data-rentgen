# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SQLQueryDTO:
    query: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.query,)

    def merge(self, new: SQLQueryDTO) -> SQLQueryDTO:
        if new.id is None:
            return self

        return SQLQueryDTO(
            query=self.query,
            id=new.id or self.id,
        )
