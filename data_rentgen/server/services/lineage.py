# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Literal

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
        start_node_ids: list[int],
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by jobs %r, with direction %s, since %s, until %s, depth %s",
                sorted(start_node_ids),
                direction,
                since,
                until,
                depth,
            )
        jobs = await self._uow.job.list_by_ids(start_node_ids)
        if not jobs:
            logger.info("No jobs found")
            return LineageServiceResult()

        jobs_by_id = {job.id: job for job in jobs}

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_job_ids(
                sorted(jobs_by_id.keys()),
                since=since,
                until=until,
                granularity=granularity,
            )
        else:
            inputs = await self._uow.input.list_by_job_ids(
                sorted(jobs_by_id.keys()),
                since=since,
                until=until,
                granularity=granularity,
            )

        ids_to_skip = ids_to_skip or IdsToSkip()
        # Return only operations which have at least one input or output
        input_operation_ids = {input.operation_id for input in inputs if input.operation_id is not None}
        output_operation_ids = {output.operation_id for output in outputs if output.operation_id is not None}
        operation_ids = input_operation_ids | output_operation_ids - ids_to_skip.operations
        operations = await self._uow.operation.list_by_ids(sorted(operation_ids))
        operations_by_id = {operation.id: operation for operation in operations}

        # Same for runs
        run_ids = {operation.run_id for operation in operations} - ids_to_skip.runs
        runs = await self._uow.run.list_by_ids(sorted(run_ids))
        runs_by_id = {run.id: run for run in runs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            inputs=inputs,
            outputs=outputs,
        )

        dataset_ids = sorted(
            {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs} - ids_to_skip.datasets,
        )
        if depth > 1:
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=dataset_ids,
                    direction=direction,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            # datasets and symlinks will be populated by nested call
            datasets = await self._uow.dataset.list_by_ids(dataset_ids)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids)
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

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

    async def get_lineage_by_runs(  # noqa: WPS217
        self,
        start_node_ids: list[UUID],
        direction: LineageDirectionV1,
        granularity: Literal["OPERATION", "RUN"],
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by runs %r, with direction %s, since %s, until %s, depth %s",
                sorted(start_node_ids),
                direction,
                since,
                until,
                depth,
            )
        runs = await self._uow.run.list_by_ids(start_node_ids)
        if not runs:
            logger.info("No runs found")
            return LineageServiceResult()

        runs_by_id = {run.id: run for run in runs}

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_run_ids(
                sorted(runs_by_id.keys()),
                since=since,
                until=until,
                granularity=granularity,
            )
        else:
            inputs = await self._uow.input.list_by_run_ids(
                sorted(runs_by_id.keys()),
                since=since,
                until=until,
                granularity=granularity,
            )

        # Return only operations which have at least one input or output
        # In case granularity == "RUN" operations will be empty
        ids_to_skip = ids_to_skip or IdsToSkip()
        input_operation_ids = {input.operation_id for input in inputs if input.operation_id is not None}
        output_operation_ids = {output.operation_id for output in outputs if output.operation_id is not None}
        operation_ids = input_operation_ids | output_operation_ids - ids_to_skip.operations
        operations = await self._uow.operation.list_by_ids(sorted(operation_ids))
        operations_by_id = {operation.id: operation for operation in operations}

        jobs = await self._uow.job.list_by_ids(sorted({run.job_id for run in runs} - ids_to_skip.jobs))
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            inputs=inputs,
            outputs=outputs,
        )

        dataset_ids = sorted(
            {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs} - ids_to_skip.datasets,
        )
        if depth > 1:
            # datasets and symlinks will be populated by nested call
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=dataset_ids,
                    direction=direction,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            datasets = await self._uow.dataset.list_by_ids(dataset_ids)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids)
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

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
        start_node_ids: list[UUID],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by operations %r, with direction %s, since %s, until %s, depth %s",
                sorted(start_node_ids),
                direction,
                since,
                until,
                depth,
            )

        operations = await self._uow.operation.list_by_ids(start_node_ids)
        if not operations:
            logger.info("No operations found")
            return LineageServiceResult()

        operations_by_id = {operation.id: operation for operation in operations}
        operation_ids = sorted(operations_by_id.keys())

        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_operation_ids(operation_ids)
        else:
            inputs = await self._uow.input.list_by_operation_ids(operation_ids)

        ids_to_skip = ids_to_skip or IdsToSkip()
        run_ids = {operation.run_id for operation in operations}
        runs = await self._uow.run.list_by_ids(sorted(run_ids - ids_to_skip.runs))
        runs_by_id = {run.id: run for run in runs}

        job_ids = {run.job_id for run in runs}
        jobs = await self._uow.job.list_by_ids(sorted(job_ids - ids_to_skip.jobs))
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            inputs=inputs,
            outputs=outputs,
        )

        dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
        dataset_ids_to_fetch = sorted(dataset_ids - ids_to_skip.datasets)
        if depth > 1:
            # datasets and symlinks will be populated by nested call
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=dataset_ids_to_fetch,
                    direction=direction,
                    granularity="OPERATION",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            datasets = await self._uow.dataset.list_by_ids(dataset_ids_to_fetch)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(
                sorted(dataset_ids),
                ids_to_skip,
            )
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

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

    async def get_lineage_by_datasets(  # noqa: WPS217
        self,
        start_node_ids: list[int],
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Get lineage by datasets %r, with direction %s, since %s, until %s, depth %s",
                sorted(start_node_ids),
                direction,
                since,
                until,
                depth,
            )

        datasets = await self._uow.dataset.list_by_ids(start_node_ids)
        if not datasets:
            logger.info("No datasets found")
            return LineageServiceResult()

        datasets_by_id = {dataset.id: dataset for dataset in datasets}

        # Threat dataset symlinks like they are specified in `start_node_ids`
        ids_to_skip = ids_to_skip or IdsToSkip()
        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(
            sorted(datasets_by_id.keys()),
            ids_to_skip,
        )
        datasets.extend(extra_datasets)

        datasets_by_id.update({dataset.id: dataset for dataset in extra_datasets})
        dataset_symlinks_by_id = {
            (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
            for dataset_symlink in dataset_symlinks
        }

        # Get datasets -> (inputs, outputs) -> operations -> runs -> jobs
        match granularity:
            case "OPERATION":
                result = await self._dataset_lineage_with_operation_granularity(
                    datasets_by_id=datasets_by_id,
                    dataset_symlinks_by_id=dataset_symlinks_by_id,
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth,
                    ids_to_skip=ids_to_skip,
                )
            case "RUN":
                result = await self._dataset_lineage_with_run_granularity(
                    datasets_by_id=datasets_by_id,
                    dataset_symlinks_by_id=dataset_symlinks_by_id,
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth,
                    ids_to_skip=ids_to_skip,
                )
            case "JOB":
                result = await self._dataset_lineage_with_job_granularity(
                    datasets_by_id=datasets_by_id,
                    dataset_symlinks_by_id=dataset_symlinks_by_id,
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth,
                    ids_to_skip=ids_to_skip,
                )
            case _:
                raise ValueError(f"Unknown granularity: {granularity}")

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
        dataset_ids: list[int],
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

        datasets_from_symlinks = await self._uow.dataset.list_by_ids(sorted(dataset_ids_to_load))
        return datasets_from_symlinks, dataset_symlinks

    async def _dataset_lineage_with_operation_granularity(
        self,
        datasets_by_id: dict[int, Dataset],
        dataset_symlinks_by_id: dict[tuple[int, int], DatasetSymlink],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        ids_to_skip = ids_to_skip or IdsToSkip()
        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            inputs = await self._uow.input.list_by_dataset_ids(
                sorted(datasets_by_id.keys() - ids_to_skip.datasets),
                since=since,
                until=until,
                granularity="OPERATION",
            )
        else:
            outputs = await self._uow.output.list_by_dataset_ids(
                sorted(datasets_by_id.keys() - ids_to_skip.datasets),
                since=since,
                until=until,
                granularity="OPERATION",
            )

        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
        )
        operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
        operation_ids_to_fetch = sorted(operation_ids - ids_to_skip.operations)

        if depth > 1:
            # operations, runs and jobs will be populated by nested call
            result.merge(
                await self.get_lineage_by_operations(
                    start_node_ids=operation_ids_to_fetch,
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            operations = await self._uow.operation.list_by_ids(operation_ids_to_fetch)
            result.operations = {operation.id: operation for operation in operations}

            run_ids = {operation.run_id for operation in operations}
            runs = await self._uow.run.list_by_ids(sorted(run_ids - ids_to_skip.runs))
            result.runs = {run.id: run for run in runs}

            job_ids = {run.job_id for run in runs}
            jobs = await self._uow.job.list_by_ids(sorted(job_ids - ids_to_skip.jobs))
            result.jobs = {job.id: job for job in jobs}

        return result

    async def _dataset_lineage_with_run_granularity(
        self,
        datasets_by_id: dict[int, Dataset],
        dataset_symlinks_by_id: dict[tuple[int, int], DatasetSymlink],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        ids_to_skip = ids_to_skip or IdsToSkip()
        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            inputs = await self._uow.input.list_by_dataset_ids(
                sorted(datasets_by_id.keys() - ids_to_skip.datasets),
                since=since,
                until=until,
                granularity="RUN",
            )
        else:
            outputs = await self._uow.output.list_by_dataset_ids(
                sorted(datasets_by_id.keys() - ids_to_skip.datasets),
                since=since,
                until=until,
                granularity="RUN",
            )
        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
        )

        run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
        run_ids_to_fetch = sorted(run_ids - ids_to_skip.runs)

        if depth > 1:
            # operations, runs and jobs will be populated by nested call
            result.merge(
                await self.get_lineage_by_runs(
                    start_node_ids=run_ids_to_fetch,
                    direction=direction,
                    granularity="RUN",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            runs = await self._uow.run.list_by_ids(sorted(run_ids_to_fetch))
            result.runs = {run.id: run for run in runs}

            job_ids = {run.job_id for run in runs}
            jobs = await self._uow.job.list_by_ids(sorted(job_ids - ids_to_skip.jobs))
            result.jobs = {job.id: job for job in jobs}

        return result

    async def _dataset_lineage_with_job_granularity(
        self,
        datasets_by_id: dict[int, Dataset],
        dataset_symlinks_by_id: dict[tuple[int, int], DatasetSymlink],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        inputs = []
        outputs = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            inputs = await self._uow.input.list_by_dataset_ids(
                sorted(datasets_by_id.keys()),
                since=since,
                until=until,
                granularity="JOB",
            )
        else:
            outputs = await self._uow.output.list_by_dataset_ids(
                sorted(datasets_by_id.keys()),
                since=since,
                until=until,
                granularity="JOB",
            )

        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs=inputs,
            outputs=outputs,
        )
        ids_to_skip = ids_to_skip or IdsToSkip()
        job_ids = {input.job_id for input in inputs} | {output.job_id for output in outputs}
        job_ids_to_fetch = sorted(job_ids - ids_to_skip.jobs)

        if depth > 1:
            # operations, runs and jobs will be populated by nested call
            result.merge(
                await self.get_lineage_by_jobs(
                    start_node_ids=job_ids_to_fetch,
                    direction=direction,
                    granularity="JOB",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    ids_to_skip=ids_to_skip.merge(IdsToSkip.from_result(result)),
                ),
            )
        else:
            jobs = await self._uow.job.list_by_ids(sorted(job_ids_to_fetch))
            result.jobs = {job.id: job for job in jobs}

        return result
