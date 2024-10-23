# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class LocationDTO:
    type: str
    name: str
    addresses: list[str]
    id: int | None = field(default=None, compare=False)

    @property
    def full_name(self) -> str:
        return f"{self.type}://{self.name}"
