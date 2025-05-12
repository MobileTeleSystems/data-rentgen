# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.job_type import JobTypeDTO
from data_rentgen.dto.location import LocationDTO


@dataclass
class JobDTO:
    name: str
    location: LocationDTO
    type: JobTypeDTO | None = None
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name)

    def merge(self, new: JobDTO) -> JobDTO:
        if new.id is None:
            # jobs aren't changed that much, reuse them if possible
            return self

        type_: JobTypeDTO | None
        if new.type and self.type:  # noqa: SIM108
            type_ = self.type.merge(new.type)
        else:
            type_ = new.type or self.type

        return JobDTO(
            location=self.location.merge(new.location),
            name=self.name,
            type=type_,
            id=new.id or self.id,
        )
