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
    RunNode,
)
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy
from data_rentgen.utils import UUID


class RunStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.RUN

    async def get_lineage(
        self,
        point_id: int | UUID,
        granularity: LineageGranularity,
        direction: str,
        depth: int,
        since: datetime,
        until: datetime | None,
    ) -> LineageResponseV1:
        await self._check_granularity(granularity)
        direction_type = await self._get_direction(direction)
        lineage = LineageResponseV1()
        run_operations = await self._uow.run.get_run_operations(point_id, since, until)  # type: ignore[arg-type]
        for run_id, operation_id in run_operations:
            operation_datasets = await self._uow.operation.get_operation_datasets(
                operation_id,
                direction_type,
                since,
                until,
            )

            if operation_datasets:
                # Add Run -> Operation and Run as Node only if operation interact with dataset
                lineage.relations.append(LineageRelation(type="parent", from_=run_id, to=operation_id))
                run_node = await self._uow.run.get_node_info(run_id)
                lineage.nodes.append(RunNode(id=run_node.id, job_name=run_node.name, status=run_node.status))

            for operation_dataset in operation_datasets:
                dataset_id = operation_dataset.dataset_id
                # Add Operation <-> Dataset
                if direction == "from":
                    lineage.relations.append(
                        LineageRelation(
                            from_=operation_id,
                            to=dataset_id,
                            type=operation_dataset.interaction_type.value,
                        ),
                    )
                elif direction == "to":
                    lineage.relations.append(
                        LineageRelation(
                            from_=dataset_id,
                            to=operation_id,
                            type=operation_dataset.interaction_type.value,
                        ),
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
