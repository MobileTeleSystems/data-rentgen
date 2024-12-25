# SPDX-FileCopyrightText: 2024 MTS PJSC
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
        return (json.dumps(self.fields, sort_keys=True),)

    def merge(self, new: SchemaDTO) -> SchemaDTO:
        return SchemaDTO(
            fields=self.fields,
            id=new.id or self.id,
        )
