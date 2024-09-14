# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Iterable

from fastapi import Depends
from uuid6 import UUID

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    Input,
    Job,
    Operation,
    Output,
    Run,
)
from data_rentgen.server.schemas.v1.lineage import LineageDirectionV1
from data_rentgen.services.uow import UnitOfWork

logger = logging.getLogger(__name__)


@dataclass
class LineageServiceResult:
    jobs: dict[int, Job] = field(default_factory=dict)
    runs: dict[UUID, Run] = field(default_factory=dict)
    operations: dict[UUID, Operation] = field(default_factory=dict)
    datasets: dict[int, Dataset] = field(default_factory=dict)
    dataset_symlinks: dict[tuple[int, int], DatasetSymlink] = field(default_factory=dict)
    inputs: list[Input] = field(default_factory=list)
    outputs: list[Output] = field(default_factory=list)

    def merge(self, other: "LineageServiceResult") -> "LineageServiceResult":
        self.jobs.update(other.jobs)
        self.runs.update(other.runs)
        self.operations.update(other.operations)
        self.datasets.update(other.datasets)
        self.dataset_symlinks.update(other.dataset_symlinks)
        self.inputs.extend(other.inputs)
        self.outputs.extend(other.outputs)
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

        all_runs = await self._uow.run.list_by_job_ids(jobs_by_id.keys(), since, until)
        run_ids = {run.id for run in all_runs}

        all_operations = await self._uow.operation.list_by_run_ids(run_ids, since, until)
        operations_ids = {operation.id for operation in all_operations}

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_operation_ids(
                operations_ids,
                since,
                until,
            )
        else:
            inputs = await self._uow.input.list_by_operation_ids(
                operations_ids,
                since,
                until,
            )

        dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
        datasets = await self._uow.dataset.list_by_ids(dataset_ids)

        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids)
        datasets.extend(extra_datasets)

        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        dataset_symlinks_by_id = {
            (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
            for dataset_symlink in dataset_symlinks
        }

        # Return only operations which have at least one input or output
        operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
        operations = [operation for operation in all_operations if operation.id in operation_ids]
        operations_by_id = {operation.id: operation for operation in operations}

        # Same for runs
        run_ids = {operation.run_id for operation in operations}
        runs = [run for run in all_runs if run.id in run_ids]
        runs_by_id = {run.id: run for run in runs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
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
                "Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, %d inputs, %d outputs",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
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

        all_operations = await self._uow.operation.list_by_run_ids(runs_by_id.keys(), since, until)
        operation_ids = {operation.id for operation in all_operations}

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_operation_ids(
                operation_ids,
                since,
                until,
            )
        else:
            inputs = await self._uow.input.list_by_operation_ids(
                operation_ids,
                since,
                until,
            )

        dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
        datasets = await self._uow.dataset.list_by_ids(dataset_ids)

        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids)
        datasets.extend(extra_datasets)

        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        dataset_symlinks_by_id = {
            (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
            for dataset_symlink in dataset_symlinks
        }

        # Return only operations which have at least one input or output
        operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
        operations = [operation for operation in all_operations if operation.id in operation_ids]
        operations_by_id = {operation.id: operation for operation in operations}

        jobs = await self._uow.job.list_by_ids({run.job_id for run in runs})
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
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
                "Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, %d inputs, %d outputs",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
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

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_operation_ids(
                operations_by_id.keys(),
                since,
                until,
            )
        else:
            inputs = await self._uow.input.list_by_operation_ids(
                operations_by_id.keys(),
                since,
                until,
            )

        ids_to_skip = ids_to_skip or IdsToSkip()
        dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
        datasets = await self._uow.dataset.list_by_ids(dataset_ids - ids_to_skip.datasets)

        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(
            dataset_ids,
            ids_to_skip,
        )
        datasets.extend(extra_datasets)

        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        dataset_symlinks_by_id = {
            (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
            for dataset_symlink in dataset_symlinks
        }

        run_ids = {operation.run_id for operation in operations}
        runs = await self._uow.run.list_by_ids(run_ids - ids_to_skip.runs)
        runs_by_id = {run.id: run for run in runs}

        job_ids = {run.job_id for run in runs}
        jobs = await self._uow.job.list_by_ids(job_ids - ids_to_skip.jobs)
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
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
                "Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, %d inputs, %d outputs",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
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

        # Threat dataset symlinks like they are specified in `point_ids`
        ids_to_skip = ids_to_skip or IdsToSkip()
        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(
            datasets_by_id.keys(),
            ids_to_skip,
        )
        datasets.extend(extra_datasets)

        datasets_by_id.update({dataset.id: dataset for dataset in extra_datasets})
        dataset_symlinks_by_id = {
            (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
            for dataset_symlink in dataset_symlinks
        }

        # Get datasets -> (inputs, outputs) -> operations -> runs -> jobs
        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            inputs = await self._uow.input.list_by_dataset_ids(
                datasets_by_id.keys(),
                since,
                until,
            )
        else:
            outputs = await self._uow.output.list_by_dataset_ids(
                datasets_by_id.keys(),
                since,
                until,
            )

        ids_to_skip = ids_to_skip or IdsToSkip()
        operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
        operations = await self._uow.operation.list_by_ids(operation_ids - ids_to_skip.operations)
        operations_by_id = {operation.id: operation for operation in operations}

        # Get runs and jobs only on top level, to reduce number of queries made
        run_ids = {operation.run_id for operation in operations}
        runs = await self._uow.run.list_by_ids(run_ids - ids_to_skip.runs)
        runs_by_id = {run.id: run for run in runs}

        job_ids = {run.job_id for run in runs}
        jobs = await self._uow.job.list_by_ids(job_ids - ids_to_skip.jobs)
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
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
                "Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, %d inputs, %d outputs",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
            )
        return result

    async def _extend_datasets_with_symlinks(
        self,
        dataset_ids: Iterable[int],
        ids_to_skip: IdsToSkip | None = None,
    ) -> tuple[list[Dataset], list[DatasetSymlink]]:
        # For now return all symlinks regardless of direction
        dataset_symlinks = await self._uow.dataset_symlink.list_by_dataset_ids(dataset_ids)

        ids_to_skip = ids_to_skip or IdsToSkip()
        dataset_ids_from_symlinks = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
        dataset_ids_to_symlinks = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
        dataset_ids_to_load = (
            (dataset_ids_from_symlinks | dataset_ids_to_symlinks) - set(dataset_ids) - ids_to_skip.datasets
        )

        datasets_from_symlinks = await self._uow.dataset.list_by_ids(dataset_ids_to_load)
        return datasets_from_symlinks, dataset_symlinks
