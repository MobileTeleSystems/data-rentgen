# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag
from functools import cached_property

from uuid6 import UUID


class DatasetColumnRelationTypeDTO(IntFlag):
    # See https://github.com/OpenLineage/OpenLineage/blob/1.27.0/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/TransformationInfo.java#L30-L40
    # Using IntFlag to avoid messing up with ARRAY type, bitwise OR is enough
    UNKNOWN = 1

    # Direct
    IDENTITY = 2
    TRANSFORMATION = 4
    TRANSFORMATION_MASKING = 8
    AGGREGATION = 16
    AGGREGATION_MASKING = 32

    # Indirect
    FILTER = 64
    JOIN = 128
    GROUP_BY = 256
    SORT = 512
    WINDOW = 1024
    CONDITIONAL = 2048


@dataclass
class DatasetColumnRelationDTO:
    type: DatasetColumnRelationTypeDTO
    source_column: str
    target_column: str | None = None
    # id is generated using other ids combination
    fingerprint: UUID | None = None
    # description is always "", see io.openlineage.spark.agent.lifecycle.plan.column.TransformationInfo

    @cached_property
    def unique_key(self) -> tuple:
        return (  # noqa: WPS227
            self.source_column,
            self.target_column or "",
        )

    def merge(self, new: DatasetColumnRelationDTO) -> DatasetColumnRelationDTO:
        return DatasetColumnRelationDTO(
            source_column=self.source_column,
            target_column=self.target_column,
            type=self.type | new.type,
            fingerprint=new.fingerprint or self.fingerprint,
        )


def merge_dataset_column_relations(relations: list[DatasetColumnRelationDTO]) -> list[DatasetColumnRelationDTO]:
    result: dict[tuple, DatasetColumnRelationDTO] = {}
    for relation in relations:
        if relation.unique_key in result:
            existing_relation = result[relation.unique_key]
            result[relation.unique_key] = existing_relation.merge(relation)
        else:
            result[relation.unique_key] = relation

    return sorted(result.values(), key=lambda item: item.unique_key)
