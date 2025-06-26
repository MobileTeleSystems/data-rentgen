# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import cached_property
from uuid import UUID

from data_rentgen.utils.uuid import generate_static_uuid


@dataclass
class SchemaDTO:
    fields: list[dict]
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.digest,)

    @cached_property
    def digest(self) -> UUID:
        return generate_static_uuid(json.dumps(self.fields, sort_keys=True))

    def merge(self, new: SchemaDTO) -> SchemaDTO:
        if new.id is None:
            return self

        return SchemaDTO(
            fields=self.fields,
            id=new.id or self.id,
        )
