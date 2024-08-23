# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.lineage import (
    LineageDirection,
    LineageEntity,
    LineageEntityKind,
    LineageRelation,
    LineageRelationKind,
    LineageResponseV1,
)
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy
from data_rentgen.utils import UUID


class OperationStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.OPERATION

    async def get_lineage(
        self,
        point_id: UUID,  # type: ignore[override]
        direction: str,
        since: datetime,
        until: datetime | None,
    ):
        direction_type = self._get_direction(direction)

        operation = await self._uow.operation.get_by_id(point_id)
        if not operation:
            return LineageResponseV1()
        lineage = LineageResponseV1(nodes=[OperationResponseV1.model_validate(operation)])

        interactions = await self._uow.interaction.get_by_operations([point_id], direction_type, since, until)
        dataset_ids = [interaction.dataset_id for interaction in interactions]
        datasets_by_id = {dataset.id: dataset for dataset in await self._uow.dataset.get_by_ids(dataset_ids)}

        for dataset in datasets_by_id.values():
            lineage.nodes.append(DatasetResponseV1.model_validate(dataset))

        for interaction in interactions:
            lineage.relations.append(
                LineageRelation(
                    kind=LineageRelationKind.INTERACTION,
                    type=interaction.type.value,
                    from_=(
                        LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id)  # type: ignore[union-attr]
                        if direction == LineageDirection.FROM
                        else LineageEntity(kind=LineageEntityKind.DATASET, id=interaction.dataset_id)
                    ),
                    to=(
                        LineageEntity(kind=LineageEntityKind.DATASET, id=interaction.dataset_id)
                        if direction == LineageDirection.FROM
                        else LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id)  # type: ignore[union-attr]
                    ),
                ),
            )

        return lineage
