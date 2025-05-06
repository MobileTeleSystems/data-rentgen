from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SQLQueryDTO:
    query: str
    id: int | None = field(default=None, compare=False)

    def merge(self, new: SQLQueryDTO) -> SQLQueryDTO:
        if new.id is None:
            return self

        return SQLQueryDTO(
            query=self.query,
            id=new.id or self.id,
        )
