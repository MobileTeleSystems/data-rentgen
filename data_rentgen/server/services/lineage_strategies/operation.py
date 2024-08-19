# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.lineage import (
    DatasetNode,
    LineageEntityKind,
    LineageGranularity,
    LineageRelation,
    LineageResponseV1,
    OperationNode,
)
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy
from data_rentgen.utils import UUID


class OperationStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.OPERATION

    async def get_lineage(
        self,
        point_id: int | UUID,
        granularity: LineageGranularity,
        direction: str,
        depth: int,
        since: datetime,
        until: datetime | None,
    ):
        await self._check_granularity(granularity)
        direction_type = await self._get_direction(direction)
        operation_datasets = await self._uow.operation.get_operation_datasets(point_id, direction_type, since, until)  # type: ignore[arg-type]
        lineage = LineageResponseV1()
        for operation_dataset in operation_datasets:
            operation_id = operation_dataset.operation_id
            dataset_id = operation_dataset.dataset_id
            # Add Operation <-> Dataset
            if direction == "from":
                lineage.relations.append(
                    LineageRelation(from_=operation_id, to=dataset_id, type=operation_dataset.interaction_type.value),
                )
            elif direction == "to":
                lineage.relations.append(
                    LineageRelation(from_=dataset_id, to=operation_id, type=operation_dataset.interaction_type.value),
                )
            # Add Operation and Dataset as Nodes
            operation_node = await self._uow.operation.get_node_info(operation_id)
            lineage.nodes.append(
                OperationNode(
                    id=operation_node.id,
                    status=operation_node.status,
                    operation_type=operation_node.type,
                    name=operation_node.name,
                ),
            )
            dataset_node = await self._uow.dataset.get_node_info(dataset_id)
            lineage.nodes.append(DatasetNode(id=dataset_node.id, name=dataset_node.name))

        return lineage
