# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
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
