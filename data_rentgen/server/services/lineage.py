# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import dataclasses
import logging
from collections import defaultdict
from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlink,
    Job,
    Operation,
    Run,
)
from data_rentgen.db.repositories.column_lineage import ColumnLineageRow
from data_rentgen.db.repositories.input import InputRow
from data_rentgen.db.repositories.output import OutputRow
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
    inputs: dict[tuple[int, int, UUID | None, UUID | None], InputRow] = field(default_factory=dict)
    outputs: dict[tuple[int, int, UUID | None, UUID | None, int | None], OutputRow] = field(default_factory=dict)
    column_lineage: dict[tuple[int, int], list[ColumnLineageRow]] = field(default_factory=dict)

    def merge(self, other: "LineageServiceResult") -> "LineageServiceResult":
        self.jobs.update(other.jobs)
        self.runs.update(other.runs)
        self.operations.update(other.operations)
        self.datasets.update(other.datasets)
        self.dataset_symlinks.update(other.dataset_symlinks)
        self.inputs.update(other.inputs)
        self.outputs.update(other.outputs)
        self.column_lineage.update(other.column_lineage)
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


class LineageService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def get_lineage_by_jobs(  # noqa: C901, PLR0912, PLR0915
        self,
        start_node_ids: Collection[int],
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,  # noqa: FBT001, FBT002
        ids_to_skip: IdsToSkip | None = None,
        level: int = 0,
    ) -> LineageServiceResult:
        if not start_node_ids:
            return LineageServiceResult()

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by runs %r, with direction %s, since %s, until %s",
                level,
                sorted(start_node_ids),
                direction,
                since,
                until,
            )

        jobs = await self._uow.job.list_by_ids(start_node_ids)
        if not jobs:
            logger.info("[Level %d] No jobs found", level)
            return LineageServiceResult()

        # Always include all requested jobs.
        jobs_by_id = {job.id: job for job in jobs}

        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            outputs.extend(
                await self._uow.output.list_by_job_ids(
                    jobs_by_id,
                    since=since,
                    until=until,
                    granularity=granularity,
                ),
            )
            if granularity != "JOB":
                # include sum over all runs
                outputs.extend(
                    await self._uow.output.list_by_job_ids(
                        jobs_by_id,
                        since=since,
                        until=until,
                        granularity="JOB",
                    ),
                )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            inputs.extend(
                await self._uow.input.list_by_job_ids(
                    jobs_by_id,
                    since=since,
                    until=until,
                    granularity=granularity,
                ),
            )
            if granularity != "JOB":
                # include sum over all runs
                inputs.extend(
                    await self._uow.input.list_by_job_ids(
                        jobs_by_id,
                        since=since,
                        until=until,
                        granularity="JOB",
                    ),
                )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        # Include only runs which have at least one input or output.
        # In case granularity == "JOB", all run_id are None.
        ids_to_skip = ids_to_skip or IdsToSkip()
        input_run_ids = {input_.run_id for input_ in inputs if input_.run_id is not None}
        output_run_ids = {output.run_id for output in outputs if output.run_id is not None}
        run_ids = input_run_ids | output_run_ids - ids_to_skip.runs
        runs = await self._uow.run.list_by_ids(run_ids)
        runs_by_id = {run.id: run for run in runs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        upstream_dataset_ids = {input_.dataset_id for input_ in inputs} - ids_to_skip.datasets
        downstream_dataset_ids = {output.dataset_id for output in outputs} - ids_to_skip.datasets
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `dataset1 -> current_job -> dataset2``,
            # but do not include relations like `another_job -> dataset2`.
            # Also datasets and symlinks will be populated by nested calls.
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=upstream_dataset_ids,
                    direction=LineageDirectionV1.UPSTREAM,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=downstream_dataset_ids,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            # datasets and symlinks will be populated by nested call
            dataset_ids = upstream_dataset_ids | downstream_dataset_ids
            datasets = await self._uow.dataset.list_by_ids(dataset_ids)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids, ids_to_skip)
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

        if level == 0:
            if include_column_lineage:
                result.column_lineage.update(
                    await self._get_column_lineage(
                        current_result=result,
                        since=since,
                        until=until,
                        granularity="JOB",
                    ),
                )

            result = self._drop_unused_datasets(result)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                level,
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def get_lineage_by_runs(  # noqa: C901, PLR0915, PLR0912
        self,
        start_node_ids: Collection[UUID],
        direction: LineageDirectionV1,
        granularity: Literal["OPERATION", "RUN"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,  # noqa: FBT001, FBT002
        ids_to_skip: IdsToSkip | None = None,
        level: int = 0,
    ) -> LineageServiceResult:
        if not start_node_ids:
            return LineageServiceResult()

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by runs %r, with direction %s, since %s, until %s",
                level,
                sorted(start_node_ids),
                direction,
                since,
                until,
            )

        runs = await self._uow.run.list_by_ids(start_node_ids)
        if not runs:
            logger.info("[Level %d] No runs found", level)
            return LineageServiceResult()

        # Always include all requested runs.
        runs_by_id = {run.id: run for run in runs}

        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            outputs.extend(
                await self._uow.output.list_by_run_ids(
                    runs_by_id,
                    since=since,
                    until=until,
                    granularity=granularity,
                ),
            )
            if granularity != "RUN":
                # include sum over all operation
                outputs.extend(
                    await self._uow.output.list_by_run_ids(
                        runs_by_id,
                        since=since,
                        until=until,
                        granularity="RUN",
                    ),
                )
            # include sum over all runs
            outputs.extend(
                await self._uow.output.list_by_run_ids(
                    runs_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            inputs.extend(
                await self._uow.input.list_by_run_ids(
                    runs_by_id,
                    since=since,
                    until=until,
                    granularity=granularity,
                ),
            )
            if granularity != "RUN":
                # include sum over all operation
                inputs.extend(
                    await self._uow.input.list_by_run_ids(
                        runs_by_id,
                        since=since,
                        until=until,
                        granularity="RUN",
                    ),
                )
            # include sum over all runs
            inputs.extend(
                await self._uow.input.list_by_run_ids(
                    runs_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        # Include only operations which have at least one input or output.
        # In case granularity == "RUN", all operation_id are None.
        ids_to_skip = ids_to_skip or IdsToSkip()
        input_operation_ids = {input_.operation_id for input_ in inputs if input_.operation_id is not None}
        output_operation_ids = {output.operation_id for output in outputs if output.operation_id is not None}
        operation_ids = input_operation_ids | output_operation_ids - ids_to_skip.operations
        operations = await self._uow.operation.list_by_ids(operation_ids)
        operations_by_id = {operation.id: operation for operation in operations}

        job_ids = {run.job_id for run in runs} - ids_to_skip.jobs
        jobs = await self._uow.job.list_by_ids(job_ids)
        jobs_by_id = {job.id: job for job in jobs}

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        upstream_dataset_ids = {input_.dataset_id for input_ in inputs} - ids_to_skip.datasets
        downstream_dataset_ids = {output.dataset_id for output in outputs} - ids_to_skip.datasets
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `dataset1 -> current_run -> dataset2``,
            # but do not include relations like `another_run -> dataset2`.
            # Also datasets and symlinks will be populated by nested calls.
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=upstream_dataset_ids,
                    direction=LineageDirectionV1.UPSTREAM,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=downstream_dataset_ids,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            dataset_ids = upstream_dataset_ids | downstream_dataset_ids
            datasets = await self._uow.dataset.list_by_ids(dataset_ids)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(dataset_ids, ids_to_skip)
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

        if level == 0:
            if include_column_lineage:
                result.column_lineage.update(
                    await self._get_column_lineage(current_result=result, since=since, until=until, granularity="RUN"),
                )

            result = self._drop_unused_datasets(result)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                level,
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def get_lineage_by_operations(  # noqa: C901, PLR0912, PLR0915
        self,
        start_node_ids: Collection[UUID],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,  # noqa: FBT001, FBT002
        ids_to_skip: IdsToSkip | None = None,
        level: int = 0,
    ) -> LineageServiceResult:
        if not start_node_ids:
            return LineageServiceResult()

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by operations %r, with direction %s, since %s, until %s",
                level,
                sorted(start_node_ids),
                direction,
                since,
                until,
            )

        operations = await self._uow.operation.list_by_ids(start_node_ids)
        if not operations:
            logger.info("[Level %d] No operations found", level)
            return LineageServiceResult()

        # Always include all requested operations.
        operations_by_id = {operation.id: operation for operation in operations}

        # Also include all parent runs & jobs.
        ids_to_skip = ids_to_skip or IdsToSkip()
        run_ids = {operation.run_id for operation in operations}
        runs = await self._uow.run.list_by_ids(run_ids - ids_to_skip.runs)
        runs_by_id = {run.id: run for run in runs}

        job_ids = {run.job_id for run in runs}
        jobs = await self._uow.job.list_by_ids(job_ids - ids_to_skip.jobs)
        jobs_by_id = {job.id: job for job in jobs}

        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            outputs.extend(
                await self._uow.output.list_by_operation_ids(operations_by_id, granularity="OPERATION"),
            )
            # include sum over all operations
            outputs.extend(
                await self._uow.output.list_by_operation_ids(operations_by_id, granularity="RUN"),
            )
            # include sum over all runs
            outputs.extend(
                await self._uow.output.list_by_operation_ids(operations_by_id, granularity="JOB"),
            )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            inputs.extend(
                await self._uow.input.list_by_operation_ids(operations_by_id, granularity="OPERATION"),
            )
            # include sum over all operations
            inputs.extend(
                await self._uow.input.list_by_operation_ids(operations_by_id, granularity="RUN"),
            )
            # include sum over all runs
            inputs.extend(
                await self._uow.input.list_by_operation_ids(operations_by_id, granularity="JOB"),
            )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        result = LineageServiceResult(
            jobs=jobs_by_id,
            runs=runs_by_id,
            operations=operations_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        upstream_dataset_ids = {input_.dataset_id for input_ in inputs} - ids_to_skip.datasets
        downstream_dataset_ids = {output.dataset_id for output in outputs} - ids_to_skip.datasets
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `dataset1 -> current_operation -> dataset2``,
            # but do not include relations like `another_operation -> dataset2`.
            # Also datasets and symlinks will be populated by nested calls.
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=upstream_dataset_ids,
                    direction=LineageDirectionV1.UPSTREAM,
                    granularity="OPERATION",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_datasets(
                    start_node_ids=downstream_dataset_ids,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    granularity="OPERATION",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            dataset_ids = upstream_dataset_ids | downstream_dataset_ids
            datasets = await self._uow.dataset.list_by_ids(dataset_ids)

            extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(
                dataset_ids,
                ids_to_skip,
            )
            datasets.extend(extra_datasets)

            result.datasets = {dataset.id: dataset for dataset in datasets}
            result.dataset_symlinks = {
                (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id): dataset_symlink
                for dataset_symlink in dataset_symlinks
            }

        if level == 0:
            if include_column_lineage:
                result.column_lineage.update(
                    await self._get_column_lineage(
                        current_result=result,
                        since=since,
                        until=until,
                        granularity="OPERATION",
                    ),
                )

            result = self._drop_unused_datasets(result)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                level,
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def get_lineage_by_datasets(
        self,
        start_node_ids: Collection[int],
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,  # noqa: FBT001, FBT002
        ids_to_skip: IdsToSkip | None = None,
        level: int = 0,
    ) -> LineageServiceResult:
        if not start_node_ids:
            return LineageServiceResult()

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by datasets %r, with direction %s, since %s, until %s",
                level,
                sorted(start_node_ids),
                direction,
                since,
                until,
            )

        datasets = await self._uow.dataset.list_by_ids(start_node_ids)
        if not datasets:
            logger.info("[Level %d] No datasets found", level)
            return LineageServiceResult()

        datasets_by_id = {dataset.id: dataset for dataset in datasets}

        # Threat dataset symlinks like they are specified in `start_node_ids`
        ids_to_skip = ids_to_skip or IdsToSkip()
        extra_datasets, dataset_symlinks = await self._extend_datasets_with_symlinks(datasets_by_id, ids_to_skip)
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
                    level=level,
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
                    level=level,
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
                    level=level,
                    ids_to_skip=ids_to_skip,
                )
            case _:
                msg = f"Unknown granularity: {granularity}"
                raise ValueError(msg)

        if level == 0:
            if include_column_lineage:
                result.column_lineage.update(
                    await self._get_column_lineage(
                        current_result=result,
                        since=since,
                        until=until,
                        granularity=granularity,
                    ),
                )

            result = self._drop_unused_datasets(result, keep_dataset_ids=start_node_ids)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                level,
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def _extend_datasets_with_symlinks(
        self,
        dataset_ids: Collection[int],
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

    async def _dataset_lineage_with_operation_granularity(
        self,
        datasets_by_id: dict[int, Dataset],
        dataset_symlinks_by_id: dict[tuple[int, int], DatasetSymlink],
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        level: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            inputs.extend(
                await self._uow.input.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="OPERATION",
                ),
            )
            # include sum over all operations
            inputs.extend(
                await self._uow.input.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="RUN",
                ),
            )
            # include sum over all runs
            inputs.extend(
                await self._uow.input.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            outputs.extend(
                await self._uow.output.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="OPERATION",
                ),
            )
            # include sum over all operations
            outputs.extend(
                await self._uow.output.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="RUN",
                ),
            )
            # include sum over all runs
            outputs.extend(
                await self._uow.output.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        ids_to_skip = ids_to_skip or IdsToSkip()
        downstream_operation_ids = {input_.operation_id for input_ in inputs if input_.operation_id is not None}
        upstream_operation_ids = {output.operation_id for output in outputs if output.operation_id is not None}

        # ignore operations only after they were included
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `operation1 -> current_dataset -> operation2``,
            # but do not include relations like `another_dataset -> operation2`.
            # Also operations, runs and jobs will be populated by nested call.
            result.merge(
                await self.get_lineage_by_operations(
                    start_node_ids=downstream_operation_ids - ids_to_skip.operations,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_operations(
                    start_node_ids=upstream_operation_ids - ids_to_skip.operations,
                    direction=LineageDirectionV1.UPSTREAM,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            operation_ids = downstream_operation_ids | upstream_operation_ids - ids_to_skip.operations
            operations = await self._uow.operation.list_by_ids(operation_ids)
            result.operations = {operation.id: operation for operation in operations}

            run_ids = {operation.run_id for operation in operations}
            runs = await self._uow.run.list_by_ids(run_ids - ids_to_skip.runs)
            result.runs = {run.id: run for run in runs}

            job_ids = {run.job_id for run in runs}
            jobs = await self._uow.job.list_by_ids(job_ids - ids_to_skip.jobs)
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
        level: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            inputs.extend(
                await self._uow.input.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="RUN",
                ),
            )
            # include sum over all runs
            inputs.extend(
                await self._uow.input.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            outputs.extend(
                await self._uow.output.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="RUN",
                ),
            )
            # include sum over all runs
            outputs.extend(
                await self._uow.output.list_by_dataset_ids(
                    datasets_by_id,
                    since=since,
                    until=until,
                    granularity="JOB",
                ),
            )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}
        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        ids_to_skip = ids_to_skip or IdsToSkip()
        downstream_run_ids = {input_.run_id for input_ in inputs if input_.run_id is not None}
        upstream_run_ids = {output.run_id for output in outputs if output.run_id is not None}
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `run1 -> current_dataset -> run2``,
            # but do not include relations like `another_dataset -> run2`.
            # Also runs and jobs will be populated by nested call.
            result.merge(
                await self.get_lineage_by_runs(
                    start_node_ids=downstream_run_ids - ids_to_skip.runs,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    granularity="RUN",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_runs(
                    start_node_ids=upstream_run_ids - ids_to_skip.runs,
                    direction=LineageDirectionV1.UPSTREAM,
                    granularity="RUN",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            run_ids = downstream_run_ids | upstream_run_ids - ids_to_skip.runs
            runs = await self._uow.run.list_by_ids(run_ids)
            result.runs = {run.id: run for run in runs}

            job_ids = {run.job_id for run in runs}
            jobs = await self._uow.job.list_by_ids(job_ids - ids_to_skip.jobs)
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
        level: int,
        ids_to_skip: IdsToSkip | None = None,
    ) -> LineageServiceResult:
        inputs: list[InputRow] = []
        outputs: list[OutputRow] = []
        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            inputs = await self._uow.input.list_by_dataset_ids(
                datasets_by_id,
                since=since,
                until=until,
                granularity="JOB",
            )

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            outputs = await self._uow.output.list_by_dataset_ids(
                datasets_by_id,
                since=since,
                until=until,
                granularity="JOB",
            )

        input_schema_ids = {input_.schema_id for input_ in inputs if input_.schema_id is not None}
        output_schema_ids = {output.schema_id for output in outputs if output.schema_id is not None}
        schemas = await self._uow.schema.list_by_ids(input_schema_ids | output_schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)

        result = LineageServiceResult(
            datasets=datasets_by_id,
            dataset_symlinks=dataset_symlinks_by_id,
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in outputs
            },
        )

        ids_to_skip = ids_to_skip or IdsToSkip()
        downstream_job_ids = {input_.job_id for input_ in inputs if input_.job_id is not None}
        upstream_job_ids = {output.job_id for output in outputs if output.job_id is not None}
        ids_to_skip = ids_to_skip.merge(IdsToSkip.from_result(result))

        if depth > 1:
            # If we passed direction=BOTH, return only relations like `job1 -> current_dataset -> job2``,
            # but do not include relations like `another_dataset -> job2`.
            # Also jobs will be populated by nested call.
            result.merge(
                await self.get_lineage_by_jobs(
                    start_node_ids=downstream_job_ids - ids_to_skip.jobs,
                    direction=LineageDirectionV1.DOWNSTREAM,
                    granularity="JOB",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
            result.merge(
                await self.get_lineage_by_jobs(
                    start_node_ids=upstream_job_ids - ids_to_skip.jobs,
                    direction=LineageDirectionV1.UPSTREAM,
                    granularity="JOB",
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                    ids_to_skip=ids_to_skip,
                ),
            )
        else:
            job_ids = downstream_job_ids | upstream_job_ids - ids_to_skip.jobs
            jobs = await self._uow.job.list_by_ids(job_ids)
            result.jobs = {job.id: job for job in jobs}

        return result

    async def _get_column_lineage(
        self,
        current_result: LineageServiceResult,
        since: datetime,
        until: datetime | None,
        granularity: Literal["OPERATION", "RUN", "JOB"],
    ) -> dict[tuple[int, int], list[ColumnLineageRow]]:
        """
        'granularity' argument of this function not the same as granularity in api request.
        Here it's used for aggregation entity
        """
        result: dict[tuple[int, int], list[ColumnLineageRow]] = defaultdict(list)
        if not current_result.inputs or not current_result.outputs:
            return result

        source_dataset_ids = [input_.dataset_id for input_ in current_result.inputs.values()]
        target_dataset_ids = [output.dataset_id for output in current_result.outputs.values()]

        match granularity:
            case "OPERATION":
                column_lineage_result = await self._uow.column_lineage.list_by_operation_ids(
                    operation_ids=current_result.operations,
                    # return column lineage only for datasets included into response
                    source_dataset_ids=source_dataset_ids,
                    target_dataset_ids=target_dataset_ids,
                )
            case "RUN":
                column_lineage_result = await self._uow.column_lineage.list_by_run_ids(
                    run_ids=current_result.runs,
                    since=since,
                    until=until,
                    source_dataset_ids=source_dataset_ids,
                    target_dataset_ids=target_dataset_ids,
                )
            case "JOB":
                column_lineage_result = await self._uow.column_lineage.list_by_job_ids(
                    job_ids=current_result.jobs,
                    since=since,
                    until=until,
                    source_dataset_ids=source_dataset_ids,
                    target_dataset_ids=target_dataset_ids,
                )
            case _:
                msg = f"Unknown granularity for column lineage: {granularity}"
                raise ValueError(msg)

        for relation in column_lineage_result:
            result[(relation.source_dataset_id, relation.target_dataset_id)].append(
                relation,
            )

        return result

    def _drop_unused_datasets(
        self,
        current_result: LineageServiceResult,
        keep_dataset_ids: Collection[int] | None = None,
    ) -> LineageServiceResult:
        dataset_ids_with_inputs = {input_.dataset_id for input_ in current_result.inputs.values()}
        dataset_ids_with_outputs = {output.dataset_id for output in current_result.outputs.values()}

        keep_ids = set(keep_dataset_ids or []) | dataset_ids_with_inputs | dataset_ids_with_outputs

        final_datasets = {key: dataset for key, dataset in current_result.datasets.items() if dataset.id in keep_ids}
        final_symlinks = {
            key: symlink
            for key, symlink in current_result.dataset_symlinks.items()
            if symlink.from_dataset_id in keep_ids and symlink.to_dataset_id in keep_ids
        }

        return dataclasses.replace(
            current_result,
            datasets=final_datasets,
            dataset_symlinks=final_symlinks,
        )
