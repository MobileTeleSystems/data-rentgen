# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.location import LocationDTO


@dataclass(slots=True)
class DatasetDTO:
    location: LocationDTO
    name: str
    format: str | None = None
    id: int | None = field(default=None, compare=False)

    @property
    def full_name(self) -> str:
        return f"{self.location.full_name}/{self.name}"  # noqa: WPS237
