# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class SchemaDTO:
    fields: list[dict]
    id: int | None = field(default=None, compare=False)
