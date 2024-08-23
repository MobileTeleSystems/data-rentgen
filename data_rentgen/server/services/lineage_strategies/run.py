# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.lineage import (
    LineageEntity,
    LineageEntityKind,
    LineageRelation,
    LineageRelationKind,
    LineageResponseV1,
)
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.schemas.v1.run import RunResponseV1
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy
from data_rentgen.utils import UUID


class RunStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.RUN

    async def get_lineage(
        self,
        point_id: UUID,  # type: ignore[override]
        direction: str,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        direction_type = self._get_direction(direction)
        run = await self._uow.run.get_by_id(point_id)

        lineage = LineageResponseV1(nodes=[RunResponseV1.model_validate(run)])
        all_operations = await self._uow.operation.get_by_run_ids([point_id], since, until)

        interactions = await self._uow.interaction.get_by_operations(
            [operation.id for operation in all_operations],
            direction_type,
            since,
            until,
        )

        operations_ids = [interaction.operation_id for interaction in interactions]
        operations_by_id = {operation.id: operation for operation in all_operations if operation.id in operations_ids}

        datasets_ids = [interaction.dataset_id for interaction in interactions]
        datasets = {dataset.id: dataset for dataset in await self._uow.dataset.get_by_ids(datasets_ids)}

        for operation in operations_by_id.values():
            lineage.relations.append(
                LineageRelation(
                    kind=LineageRelationKind.PARENT,
                    from_=LineageEntity(kind=LineageEntityKind.RUN, id=run.id),  # type: ignore[union-attr]
                    to=LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id),
                ),
            )
            lineage.nodes.append(OperationResponseV1.model_validate(operation))

        for dataset in datasets.values():
            lineage.nodes.append(DatasetResponseV1.model_validate(dataset))

        for interaction in interactions:

            lineage.relations.append(
                LineageRelation(
                    kind=LineageRelationKind.INTERACTION,
                    type=interaction.type.value,
                    from_=(
                        LineageEntity(kind=LineageEntityKind.OPERATION, id=interaction.operation_id)
                        if direction == "FROM"
                        else LineageEntity(kind=LineageEntityKind.DATASET, id=interaction.dataset_id)
                    ),
                    to=(
                        LineageEntity(kind=LineageEntityKind.DATASET, id=interaction.dataset_id)
                        if direction == "FROM"
                        else LineageEntity(kind=LineageEntityKind.OPERATION, id=interaction.operation_id)
                    ),
                ),
            )

        return lineage
