# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.lineage import (
    LineageEntity,
    LineageEntityKind,
    LineageGranularity,
    LineageRelation,
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
        granularity: LineageGranularity,
        direction: str,
        depth: int,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        await self._check_granularity(granularity)
        direction_type = await self._get_direction(direction)
        # TODO: Add recursive logic for child runs
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
        operations = {operation.id: operation for operation in await self._uow.operation.get_by_ids(operations_ids)}  # type: ignore[arg-type]

        datasets_ids = [interaction.dataset_id for interaction in interactions]
        datasets = {dataset.id: dataset for dataset in await self._uow.dataset.get_by_ids(datasets_ids)}

        # TODO: Add granularity logic
        for interaction in interactions:
            dataset = datasets[interaction.dataset_id]
            operation = operations[interaction.operation_id]

            lineage.relations.append(
                LineageRelation(
                    kind=interaction.type.value,
                    from_=(
                        LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id)
                        if direction == "from"
                        else LineageEntity(kind=LineageEntityKind.DATASET, id=dataset.id)
                    ),
                    to=(
                        LineageEntity(kind=LineageEntityKind.DATASET, id=dataset.id)
                        if direction == "from"
                        else LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id)
                    ),
                ),
            )

            lineage.relations.append(
                LineageRelation(
                    kind="PARENT",
                    from_=LineageEntity(kind=LineageEntityKind.RUN, id=run.id),  # type: ignore[union-attr]
                    to=LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id),
                ),
            )
            lineage.nodes.append(OperationResponseV1.model_validate(operation))
            lineage.nodes.append(DatasetResponseV1.model_validate(dataset))

        return lineage
