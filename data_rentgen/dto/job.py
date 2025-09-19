# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from copy import copy
from dataclasses import dataclass, field

from data_rentgen.dto.job_type import JobTypeDTO
from data_rentgen.dto.location import LocationDTO


@dataclass(slots=True)
class JobDTO:
    name: str
    location: LocationDTO
    type: JobTypeDTO | None = None
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name.lower())

    def merge(self, new: JobDTO) -> JobDTO:
        self.id = new.id or self.id
        self.location = self.location.merge(new.location)

        if new.type and self.type:
            self.type = self.type.merge(new.type)
        else:
            self.type = new.type or self.type

        if self.name == "unknown" and new.name != "unknown":
            # Workaround for https://github.com/OpenLineage/OpenLineage/issues/3846
            result = copy(self)
            result.name = new.name
            return result

        return self
