# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.operation import OperationDTO
from data_rentgen.dto.schema import SchemaDTO


class OutputTypeDTO(str, Enum):
    CREATE = "CREATE"
    ALTER = "ALTER"
    RENAME = "RENAME"

    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"

    DROP = "DROP"
    TRUNCATE = "TRUNCATE"

    def __str__(self) -> str:
        return self.value


@dataclass
class OutputDTO:
    operation: OperationDTO
    dataset: DatasetDTO
    type: OutputTypeDTO
    schema: SchemaDTO | None = None
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None
    # id is generated using other ids combination
    id: UUID | None = None

    @property
    def unique_key(self) -> tuple:
        return (
            self.operation.unique_key,
            self.dataset.unique_key,
            self.type,
            (self.schema.unique_key if self.schema else None),
        )

    def merge(self, new: OutputDTO) -> OutputDTO:
        schema: SchemaDTO | None
        if self.schema and new.schema:  # noqa: SIM108
            schema = self.schema.merge(new.schema)
        else:
            schema = new.schema or self.schema

        return OutputDTO(
            operation=self.operation.merge(new.operation),
            dataset=self.dataset.merge(new.dataset),
            type=self.type,
            schema=schema,
            num_rows=max(filter(None, [new.num_rows, self.num_rows]), default=None),
            num_bytes=max(filter(None, [new.num_bytes, self.num_bytes]), default=None),
            num_files=max(filter(None, [new.num_files, self.num_files]), default=None),
            id=new.id or self.id,
        )
