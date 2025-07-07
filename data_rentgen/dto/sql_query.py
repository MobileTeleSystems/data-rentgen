# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from uuid import UUID

from data_rentgen.utils.uuid import generate_static_uuid


@dataclass
class SQLQueryDTO:
    query: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.fingerprint,)

    @cached_property
    def fingerprint(self) -> UUID:
        return generate_static_uuid(self.query)

    def merge(self, new: SQLQueryDTO) -> SQLQueryDTO:
        self.id = new.id or self.id
        return self
