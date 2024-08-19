# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.lineage import (
    DatasetNode,
    JobNode,
    LineageEntityKind,
    LineageGranularity,
    LineageRelation,
    LineageResponseV1,
    OperationNode,
    RunNode,
)
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy
from data_rentgen.utils import UUID


class JobStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.JOB

    async def get_lineage(  # noqa: WPS217
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
        lineage = LineageResponseV1()

        job_runs = await self._uow.job.get_job_runs(point_id, since, until)  # type: ignore[arg-type]
        for job_id, run_id in job_runs:
            # Add Job -> Run relation, only if granularity is more then JOB
            lineage.relations.append(LineageRelation(type="parent", from_=job_id, to=run_id))
            # Add Jobe as Node
            job_node = await self._uow.job.get_node_info(job_id)
            lineage.nodes.append(JobNode(id=job_node.id, name=job_node.name, job_type=job_node.type))

            run_operations = await self._uow.run.get_run_operations(run_id, since, until)
            for _, operation_id in run_operations:
                operation_datasets = await self._uow.operation.get_operation_datasets(
                    operation_id,
                    direction_type,
                    since,
                    until,
                )

                if operation_datasets:
                    # Add Run -> Operation and Run as Node only if operation interact with dataset and granularity is equal to OPERATION
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
