# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime
from functools import cached_property
from uuid import UUID

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.dataset_column_relation import (
    DatasetColumnRelationDTO,
    merge_dataset_column_relations,
)
from data_rentgen.dto.operation import OperationDTO
from data_rentgen.utils.uuid import generate_incremental_uuid, generate_static_uuid


@dataclass
class ColumnLineageDTO:
    operation: OperationDTO
    source_dataset: DatasetDTO
    target_dataset: DatasetDTO
    dataset_column_relations: InitVar[list[DatasetColumnRelationDTO]]
    _dataset_column_relations: list[DatasetColumnRelationDTO] = field(default_factory=list, init=False)

    def __post_init__(self, dataset_column_relations: list[DatasetColumnRelationDTO]):
        self._dataset_column_relations = merge_dataset_column_relations(dataset_column_relations)

    @property
    def column_relations(self) -> list[DatasetColumnRelationDTO]:
        return self._dataset_column_relations

    @property
    def unique_key(self) -> tuple:
        return (self.operation.unique_key, self.source_dataset.unique_key, self.target_dataset.unique_key)

    def generate_id(self) -> UUID:
        # Instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id.
        # This property could be accessed only after all nested ids are fetched from DB
        id_components = [
            str(self.operation.id),
            str(self.source_dataset.id),
            str(self.target_dataset.id),
            str(self.fingerprint),
        ]
        return generate_incremental_uuid(self.created_at, ".".join(id_components))

    @property
    def created_at(self) -> datetime:
        return self.operation.created_at

    @cached_property
    def fingerprint(self) -> UUID:
        id_components = sorted((*item.unique_key, item.type) for item in self.column_relations)
        str_components = [".".join(map(str, item)) for item in id_components]
        return generate_static_uuid(",".join(str_components))

    def merge(self, new: ColumnLineageDTO) -> ColumnLineageDTO:
        return ColumnLineageDTO(
            operation=self.operation.merge(new.operation),
            source_dataset=self.source_dataset.merge(new.source_dataset),
            target_dataset=self.target_dataset.merge(new.target_dataset),
            dataset_column_relations=self.column_relations + new.column_relations,
        )
