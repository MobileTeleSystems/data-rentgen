# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import cached_property


@dataclass
class SchemaDTO:
    fields: list[dict]
    id: int | None = field(default=None, compare=False)

    @cached_property
    def unique_key(self) -> tuple:
        # expensive operation, calculate it only once
        return (json.dumps(self.fields, sort_keys=True),)

    def merge(self, new: SchemaDTO) -> SchemaDTO:
        if new.id is None:
            return self

        return SchemaDTO(
            fields=self.fields,
            id=new.id or self.id,
        )
