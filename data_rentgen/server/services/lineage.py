# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated

from fastapi import Depends

from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.interaction import Interaction
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run
from data_rentgen.dto.interaction import InteractionTypeDTO
from data_rentgen.server.schemas.v1.lineage import LineageDirectionV1
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils import UUID


@dataclass
class LineageServiceResult:
    jobs: list[Job] = field(default_factory=list)
    runs: list[Run] = field(default_factory=list)
    operations: list[Operation] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    interactions: list[Interaction] = field(default_factory=list)


# TODO: Add depth logic: DOP-18989
# TODO: Add granularity logic: DOP-18988
class LineageService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def get_lineage_by_jobs(
        self,
        point_id: int,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
    ) -> LineageServiceResult:
        jobs = await self._uow.job.list_by_ids([point_id])
        if not jobs:
            return LineageServiceResult()

        runs = await self._uow.run.list_by_job_ids({job.id for job in jobs}, since, until)
        operations = await self._uow.operation.list_by_run_ids({run.id for run in runs}, since, until)

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            {operation.id for operation in operations},
            interaction_types,
            since,
            until,
        )

        datasets = await self._uow.dataset.list_by_ids({interaction.dataset_id for interaction in interactions})
        return LineageServiceResult(
            datasets=datasets,
            interactions=interactions,
            operations=operations,
            runs=runs,
            jobs=jobs,
        )

    async def get_lineage_by_runs(
        self,
        point_id: UUID,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
    ) -> LineageServiceResult:
        runs = await self._uow.run.list_by_ids([point_id])
        if not runs:
            return LineageServiceResult()

        operations = await self._uow.operation.list_by_run_ids({run.id for run in runs}, since, until)

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            {operation.id for operation in operations},
            interaction_types,
            since,
            until,
        )

        datasets = await self._uow.dataset.list_by_ids({interaction.dataset_id for interaction in interactions})
        jobs = await self._uow.job.list_by_ids({run.job_id for run in runs})

        return LineageServiceResult(
            datasets=datasets,
            interactions=interactions,
            operations=operations,
            runs=runs,
            jobs=jobs,
        )

    async def get_lineage_by_operations(
        self,
        point_id: UUID,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
    ) -> LineageServiceResult:
        operations = await self._uow.operation.list_by_ids([point_id])
        if not operations:
            return LineageServiceResult()

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            {operation.id for operation in operations},
            interaction_types,
            since,
            until,
        )

        datasets = await self._uow.dataset.list_by_ids({interaction.dataset_id for interaction in interactions})
        runs = await self._uow.run.list_by_ids({operation.run_id for operation in operations})
        jobs = await self._uow.job.list_by_ids({run.job_id for run in runs})

        return LineageServiceResult(
            datasets=datasets,
            interactions=interactions,
            operations=operations,
            runs=runs,
            jobs=jobs,
        )

    async def get_lineage_by_datasets(
        self,
        point_id: int,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
    ) -> LineageServiceResult:
        datasets = await self._uow.dataset.list_by_ids([point_id])
        if not datasets:
            return LineageServiceResult()

        # Get datasets -> interactions -> operations -> runs -> jobs
        # Directions are inverted for datasets
        interaction_types = self.get_interaction_types(~direction)
        interactions = await self._uow.interaction.list_by_dataset_ids(
            [point_id],
            interaction_types,
            since,
            until,
        )

        operations = await self._uow.operation.list_by_ids({interaction.operation_id for interaction in interactions})
        runs = await self._uow.run.list_by_ids({operation.run_id for operation in operations})
        jobs = await self._uow.job.list_by_ids({run.job_id for run in runs})

        return LineageServiceResult(
            datasets=datasets,
            interactions=interactions,
            operations=operations,
            runs=runs,
            jobs=jobs,
        )

    def get_interaction_types(self, direction: LineageDirectionV1) -> list[str]:
        if direction == LineageDirectionV1.TO:
            return [InteractionTypeDTO.READ.value]
        return [item.value for item in InteractionTypeDTO.write_interactions()]
