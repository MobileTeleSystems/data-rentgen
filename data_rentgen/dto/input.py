# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.operation import OperationDTO
from data_rentgen.dto.schema import SchemaDTO


@dataclass(slots=True)
class InputDTO:
    operation: OperationDTO
    dataset: DatasetDTO
    schema: SchemaDTO | None = None
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None
