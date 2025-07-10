# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from data_rentgen.dto.dataset import DatasetDTO


class DatasetSymlinkTypeDTO(str, Enum):
    METASTORE = "METASTORE"
    WAREHOUSE = "WAREHOUSE"

    def __str__(self) -> str:
        return self.value


@dataclass(slots=True)
class DatasetSymlinkDTO:
    from_dataset: DatasetDTO
    to_dataset: DatasetDTO
    type: DatasetSymlinkTypeDTO
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.from_dataset.unique_key, self.to_dataset.unique_key, self.type)

    def merge(self, new: DatasetSymlinkDTO) -> DatasetSymlinkDTO:
        self.from_dataset.merge(new.from_dataset)
        self.to_dataset.merge(new.to_dataset)
        self.id = new.id or self.id
        return self
