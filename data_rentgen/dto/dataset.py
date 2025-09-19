# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.location import LocationDTO


@dataclass(slots=True)
class DatasetDTO:
    location: LocationDTO
    name: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name.lower())

    def merge(self, new: DatasetDTO) -> DatasetDTO:
        self.location.merge(new.location)
        self.id = new.id or self.id
        return self
