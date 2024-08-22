# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.lineage import (
    LineageEntity,
    LineageEntityKind,
    LineageRelation,
    LineageResponseV1,
)
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy


class DatasetStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.DATASET

    async def get_lineage(
        self,
        point_id: int,  # type: ignore[override]
        direction: str,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        # Logic are inverted for datasets
        if direction == "from":
            direction_type = self._get_direction("to")
        elif direction == "to":
            direction_type = self._get_direction("from")
        dataset = await self._uow.dataset.get_by_id(point_id)
        lineage = LineageResponseV1(nodes=[DatasetResponseV1.model_validate(dataset)])
        interactions = await self._uow.interaction.get_by_datasets([point_id], direction_type, since, until)
        operation_ids = [interaction.operation_id for interaction in interactions]
        operations_by_id = {operation.id: operation for operation in await self._uow.operation.get_by_ids(operation_ids)}  # type: ignore[arg-type]

        for operation in operations_by_id.values():
            lineage.nodes.append(OperationResponseV1.model_validate(operation))

        for interaction in interactions:
            lineage.relations.append(
                LineageRelation(
                    kind="INTERACTION",
                    type=interaction.type.value,
                    from_=(
                        LineageEntity(kind=LineageEntityKind.OPERATION, id=interaction.operation_id)
                        if direction == "to"
                        else LineageEntity(kind=LineageEntityKind.DATASET, id=dataset.id)  # type: ignore[union-attr]
                    ),
                    to=(
                        LineageEntity(kind=LineageEntityKind.DATASET, id=dataset.id)  # type: ignore[union-attr]
                        if direction == "to"
                        else LineageEntity(kind=LineageEntityKind.OPERATION, id=interaction.operation_id)
                    ),
                ),
            )

        return lineage
