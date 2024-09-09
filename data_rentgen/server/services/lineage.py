# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Iterable

from fastapi import Depends
from uuid6 import UUID

from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.interaction import Interaction
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run
from data_rentgen.dto.interaction import InteractionTypeDTO
from data_rentgen.server.schemas.v1.lineage import LineageDirectionV1
from data_rentgen.services.uow import UnitOfWork

logger = logging.getLogger(__name__)


@dataclass
class LineageServiceResult:
    jobs: dict[int, Job] = field(default_factory=dict)
    runs: dict[UUID, Run] = field(default_factory=dict)
    operations: dict[UUID, Operation] = field(default_factory=dict)
    datasets: dict[int, Dataset] = field(default_factory=dict)
    interactions: list[Interaction] = field(default_factory=list)

    def merge(self, other: "LineageServiceResult") -> "LineageServiceResult":
        self.jobs.update(other.jobs)
        self.runs.update(other.runs)
        self.operations.update(other.operations)
        self.datasets.update(other.datasets)
        self.interactions.extend(other.interactions)
        return self


@dataclass
class IdsToSkip:
    jobs: set[int] = field(default_factory=set)
    runs: set[UUID] = field(default_factory=set)
    operations: set[UUID] = field(default_factory=set)
    datasets: set[int] = field(default_factory=set)

    @classmethod
    def from_result(cls, result: LineageServiceResult) -> "IdsToSkip":
        return cls(
            jobs=set(result.jobs.keys()),
            runs=set(result.runs.keys()),
            operations=set(result.operations.keys()),
            datasets=set(result.datasets.keys()),
        )

    def merge(self, other: "IdsToSkip") -> "IdsToSkip":
        self.jobs.update(other.jobs)
        self.runs.update(other.runs)
        self.operations.update(other.operations)
        self.datasets.update(other.datasets)
        return self


# TODO: Add granularity logic: DOP-18988
class LineageService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def get_lineage_by_jobs(
        self,
        point_ids: Iterable[int],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by jobs %r, with direction %s, since %s, until %s, depth %s",
                sorted(point_ids),
                direction,
                since,
                until,
                depth,
            )
        jobs = await self._uow.job.list_by_ids(point_ids)
        jobs_by_id = {job.id: job for job in jobs}
        if not jobs:
            logger.info("No jobs found")
            return LineageServiceResult()

        runs = await self._uow.run.list_by_job_ids(jobs_by_id.keys(), since, until)
        runs_by_id = {run.id: run for run in runs}

        operations = await self._uow.operation.list_by_run_ids(runs_by_id.keys(), since, until)
        operations_by_id = {operation.id: operation for operation in operations}

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            operations_by_id.keys(),
            interaction_types,
            since,
            until,
        )

        datasets = await self._uow.dataset.list_by_ids({interaction.dataset_id for interaction in interactions})
        datasets_by_id = {dataset.id: dataset for dataset in datasets}

        result = LineageServiceResult(
            datasets=datasets_by_id,
            interactions=interactions,
            operations=operations_by_id,
            runs=runs_by_id,
            jobs=jobs_by_id,
        )
        if depth > 1:
            result.merge(
                await self.get_lineage_by_datasets(
                    point_ids=datasets_by_id.keys(),
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=IdsToSkip.from_result(result),
                ),
            )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Found %d datasets, %d interactions, %d operations, %d runs, %d jobs",
                len(result.datasets),
                len(result.interactions),
                len(result.operations),
                len(result.runs),
                len(result.jobs),
            )
        return result

    async def get_lineage_by_runs(
        self,
        point_ids: Iterable[UUID],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by runs %r, with direction %s, since %s, until %s, depth %s",
                sorted(point_ids),
                direction,
                since,
                until,
                depth,
            )

        runs = await self._uow.run.list_by_ids(point_ids)
        runs_by_id = {run.id: run for run in runs}
        if not runs:
            logger.info("No runs found")
            return LineageServiceResult()

        operations = await self._uow.operation.list_by_run_ids(runs_by_id.keys(), since, until)
        operations_by_id = {operation.id: operation for operation in operations}

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            operations_by_id.keys(),
            interaction_types,
            since,
            until,
        )

        datasets = await self._uow.dataset.list_by_ids({interaction.dataset_id for interaction in interactions})
        datasets_by_id = {dataset.id: dataset for dataset in datasets}

        jobs = await self._uow.job.list_by_ids({run.job_id for run in runs})
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            datasets=datasets_by_id,
            interactions=interactions,
            operations=operations_by_id,
            runs=runs_by_id,
            jobs=jobs_by_id,
        )
        if depth > 1:
            result.merge(
                await self.get_lineage_by_datasets(
                    point_ids=datasets_by_id.keys(),
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=IdsToSkip.from_result(result),
                ),
            )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Found %d datasets, %d interactions, %d operations, %d runs, %d jobs",
                len(result.datasets),
                len(result.interactions),
                len(result.operations),
                len(result.runs),
                len(result.jobs),
            )

        return result

    async def get_lineage_by_operations(
        self,
        point_ids: Iterable[UUID],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by operations %r, with direction %s, since %s, until %s, depth %s",
                sorted(point_ids),
                direction,
                since,
                until,
                depth,
            )

        operations = await self._uow.operation.list_by_ids(point_ids)
        operations_by_id = {operation.id: operation for operation in operations}
        if not operations:
            logger.info("No operations found")
            return LineageServiceResult()

        interaction_types = self.get_interaction_types(direction)
        interactions = await self._uow.interaction.list_by_operation_ids(
            operations_by_id.keys(),
            interaction_types,
            since,
            until,
        )

        ids_to_skip = ids_to_skip or IdsToSkip()
        dataset_ids_to_load = {interaction.dataset_id for interaction in interactions} - ids_to_skip.datasets
        datasets = await self._uow.dataset.list_by_ids(dataset_ids_to_load)
        datasets_by_id = {dataset.id: dataset for dataset in datasets}

        run_ids_to_load = {operation.run_id for operation in operations} - ids_to_skip.runs
        runs = await self._uow.run.list_by_ids(run_ids_to_load)
        runs_by_id = {run.id: run for run in runs}

        job_ids_to_load = {run.job_id for run in runs} - ids_to_skip.jobs
        jobs = await self._uow.job.list_by_ids(job_ids_to_load)
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            datasets=datasets_by_id,
            interactions=interactions,
            operations=operations_by_id,
            runs=runs_by_id,
            jobs=jobs_by_id,
        )
        if depth > 1:
            result.merge(
                await self.get_lineage_by_datasets(
                    point_ids=datasets_by_id.keys(),
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Found %d datasets, %d interactions, %d operations, %d runs, %d jobs",
                len(result.datasets),
                len(result.interactions),
                len(result.operations),
                len(result.runs),
                len(result.jobs),
            )
        return result

    async def get_lineage_by_datasets(
        self,
        point_ids: Iterable[int],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by datasets %r, with direction %s, since %s, until %s, depth %s",
                sorted(point_ids),
                direction,
                since,
                until,
                depth,
            )

        datasets = await self._uow.dataset.list_by_ids(point_ids)
        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        if not datasets:
            logger.info("No datasets found")
            return LineageServiceResult()

        # Get datasets -> interactions -> operations -> runs -> jobs
        # Directions are inverted for datasets
        interaction_types = self.get_interaction_types(~direction)
        interactions = await self._uow.interaction.list_by_dataset_ids(
            datasets_by_id.keys(),
            interaction_types,
            since,
            until,
        )

        ids_to_skip = ids_to_skip or IdsToSkip()
        operation_ids_to_load = {interaction.operation_id for interaction in interactions} - ids_to_skip.operations
        operations = await self._uow.operation.list_by_ids(operation_ids_to_load)
        operations_by_id = {operation.id: operation for operation in operations}

        # Get runs and jobs only on top level, to reduce number of queries made
        run_ids_to_load = {operation.run_id for operation in operations} - ids_to_skip.runs
        runs = await self._uow.run.list_by_ids(run_ids_to_load)
        runs_by_id = {run.id: run for run in runs}

        job_ids_to_load = {run.job_id for run in runs} - ids_to_skip.jobs
        jobs = await self._uow.job.list_by_ids(job_ids_to_load)
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            datasets=datasets_by_id,
            interactions=interactions,
            operations=operations_by_id,
            runs=runs_by_id,
            jobs=jobs_by_id,
        )
        if depth > 1:
            result.merge(
                await self.get_lineage_by_operations(
                    point_ids=operations_by_id.keys(),
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Found %d datasets, %d interactions, %d operations, %d runs, %d jobs",
                len(result.datasets),
                len(result.interactions),
                len(result.operations),
                len(result.runs),
                len(result.jobs),
            )
        return result

    def get_interaction_types(self, direction: LineageDirectionV1) -> list[str]:
        if direction == LineageDirectionV1.TO:
            return [InteractionTypeDTO.READ.value]
        return [item.value for item in InteractionTypeDTO.write_interactions()]
