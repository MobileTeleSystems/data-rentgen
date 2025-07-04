# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.location import LocationDTO


@dataclass
class DatasetDTO:
    location: LocationDTO
    name: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name)

    def merge(self, new: DatasetDTO) -> DatasetDTO:
        if self.location is new.location and new.id is None:
            # datasets aren't changed that much, reuse them if possible
            return self

        return DatasetDTO(
            location=self.location.merge(new.location),
            name=self.name,
            id=new.id or self.id,
        )
