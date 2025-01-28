# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LocationDTO:
    type: str
    name: str
    addresses: set[str]
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.type, self.name)

    def merge(self, new: LocationDTO) -> LocationDTO:
        return LocationDTO(
            type=self.type,
            name=self.name,
            addresses=self.addresses | new.addresses,
            id=new.id or self.id,
        )
