# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property

from data_rentgen.dto.location import LocationDTO


@dataclass
class DatasetDTO:
    location: LocationDTO
    name: str
    format: str | None = None
    id: int | None = field(default=None, compare=False)

    @cached_property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name)

    def merge(self, new: DatasetDTO) -> DatasetDTO:
        return DatasetDTO(
            location=self.location.merge(new.location),
            name=self.name,
            format=new.format or self.format,
            id=new.id or self.id,
        )
