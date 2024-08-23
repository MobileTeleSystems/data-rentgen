# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.job import JobResponseV1
from data_rentgen.server.schemas.v1.lineage import (
    LineageEntity,
    LineageEntityKind,
    LineageRelation,
    LineageResponseV1,
)
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.schemas.v1.run import RunResponseV1
from data_rentgen.server.services.lineage_strategies.base import AbstractStrategy


class JobStrategy(AbstractStrategy):
    point_kind = LineageEntityKind.JOB

    async def get_lineage(  # noqa: WPS217
        self,
        point_id: int,  # type: ignore[override]
        direction: str,
        since: datetime,
        until: datetime | None,
    ):
        direction_type = self._get_direction(direction)
        job = await self._uow.job.get_by_id(point_id)

        lineage = LineageResponseV1(nodes=[JobResponseV1.model_validate(job)])

        all_runs = await self._uow.run.get_by_job_id(point_id, since, until)
        all_operations = await self._uow.operation.get_by_run_ids([run.id for run in all_runs], since, until)

        interactions = await self._uow.interaction.get_by_operations(
            [operation.id for operation in all_operations],
            direction_type,
            since,
            until,
        )
        # Now we have only interactions which we need to add. So we need to filter runs with this operations.
        operations_ids = [interaction.operation_id for interaction in interactions]
        operations_by_id = {operation.id: operation for operation in all_operations if operation.id in operations_ids}  # type: ignore[arg-type]

        datasets_ids = [interaction.dataset_id for interaction in interactions]
        datasets_by_id = {dataset.id: dataset for dataset in await self._uow.dataset.get_by_ids(datasets_ids)}

        run_ids = [operation.run_id for operation in operations_by_id.values()]
        runs_by_id = {run.id: run for run in all_runs if run.id in run_ids}  # type: ignore[arg-type]

        # Add Job->Run relation
        for run_id, run in runs_by_id.items():
            lineage.relations.append(
                LineageRelation(
                    kind="PARENT",
                    from_=LineageEntity(kind=LineageEntityKind.JOB, id=job.id),  # type: ignore[union-attr]
                    to=LineageEntity(kind=LineageEntityKind.RUN, id=run_id),
                ),
            )
            lineage.nodes.append(RunResponseV1.model_validate(run))

        for operation in operations_by_id.values():
            lineage.nodes.append(OperationResponseV1.model_validate(operation))
            lineage.relations.append(
                LineageRelation(
                    kind="PARENT",
                    from_=LineageEntity(kind=LineageEntityKind.RUN, id=operation.run_id),
                    to=LineageEntity(kind=LineageEntityKind.OPERATION, id=operation.id),
                ),
            )

        for dataset in datasets_by_id.values():
            lineage.nodes.append(DatasetResponseV1.model_validate(dataset))

        for interaction in interactions:

            lineage.relations.append(
                LineageRelation(
                    kind="INTERACTION",
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
