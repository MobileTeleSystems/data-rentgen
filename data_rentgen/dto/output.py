# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import cached_property

from uuid6 import UUID

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

    @cached_property
    def unique_key(self) -> tuple:
        return (
            self.operation.unique_key,
            self.dataset.unique_key,
            self.type,
            (self.schema.unique_key if self.schema else None),
        )

    def merge(self, new: OutputDTO) -> OutputDTO:
        schema: SchemaDTO | None
        if self.schema and new.schema:
            schema = self.schema.merge(new.schema)
        else:
            schema = new.schema or self.schema

        return OutputDTO(
            operation=self.operation.merge(new.operation),
            dataset=self.dataset.merge(new.dataset),
            type=self.type,
            schema=schema,
            num_rows=new.num_rows or self.num_rows,
            num_bytes=new.num_bytes or self.num_bytes,
            num_files=new.num_files or self.num_files,
            id=new.id or self.id,
        )
