# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TypeVar

from data_rentgen.consumer.extractors.column_lineage import extract_column_lineage
from data_rentgen.consumer.extractors.input import extract_input
from data_rentgen.consumer.extractors.operation import extract_operation
from data_rentgen.consumer.extractors.output import extract_output
from data_rentgen.consumer.extractors.run import extract_run
from data_rentgen.consumer.openlineage.job_facets.job_type import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetDTO,
    DatasetSymlinkDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
    SchemaDTO,
    UserDTO,
)

T = TypeVar(
    "T",
    LocationDTO,
    DatasetDTO,
    ColumnLineageDTO,
    DatasetSymlinkDTO,
    JobDTO,
    RunDTO,
    OperationDTO,
    InputDTO,
    OutputDTO,
    SchemaDTO,
    UserDTO,
)


class BatchExtractionResult:
    """Track results of batch extraction.

    Calling any ``add_*`` method will add DTO item to the result, including nested DTOs,
    like ``OperationDTO`` -> ``RunDTO`` -> ``JobDTO`` -> ``LocationDTO``, and so on.

    Each DTO type is tracked separately. DTOs with same ``unique_key`` are merged into one final DTO,
    by calling ``existing.merge(new)``. The resulting final DTO contains all non-null attributes of original DTOs.
    Last DTO in the chain has a higher priority than previuos ones.
    For example ``RunDTO(status=STARTED, started_at=...).merge(RunDTO(status=SUCCEEDED, ended_at=...))``
    produces final ``RunDTO(status=SUCCEEDED, started_at=..., ended_at=...)``.

    Calling get methods, like ``jobs()``, will return the list of tracked DTOs with resolved
    cross-links. For example, iterating over ``jobs()`` with return the same objects
    as in ``[run.job for run in runs()]``.
    This makes modification of nested DTOs easier, all changes will be reflected in the parent DTOs as well.
    """

    def __init__(self):
        self._locations: dict[tuple, LocationDTO] = {}
        self._datasets: dict[tuple, DatasetDTO] = {}
        self._dataset_symlinks: dict[tuple, DatasetSymlinkDTO] = {}
        self._jobs: dict[tuple, JobDTO] = {}
        self._runs: dict[tuple, RunDTO] = {}
        self._operations: dict[tuple, OperationDTO] = {}
        self._inputs: dict[tuple, InputDTO] = {}
        self._outputs: dict[tuple, OutputDTO] = {}
        self._column_lineage: dict[tuple, ColumnLineageDTO] = {}
        self._schemas: dict[tuple, SchemaDTO] = {}
        self._users: dict[tuple, UserDTO] = {}

    def __repr__(self):
        return (
            "ExtractionResult("
            f"locations={len(self._locations)}, "
            f"datasets={len(self._datasets)}, "
            f"dataset_symlinks={len(self._dataset_symlinks)}, "
            f"jobs={len(self._jobs)}, "
            f"runs={len(self._runs)}, "
            f"operations={len(self._operations)}, "
            f"inputs={len(self._inputs)}, "
            f"outputs={len(self._outputs)}, "
            f"column_lineage={len(self._column_lineage)}, "
            f"schemas={len(self._schemas)}, "
            f"users={len(self._users)}"
            ")"
        )

    @staticmethod
    def _add(context: dict[tuple, T], new_item: T) -> dict[tuple, T]:
        key = new_item.unique_key
        if key in context:
            old_item = context[key]
            if old_item is new_item:
                return context

            context[key] = old_item.merge(new_item)
        else:
            context[key] = new_item
        return context

    def add_location(self, location: LocationDTO):
        self._add(self._locations, location)

    def add_dataset(self, dataset: DatasetDTO):
        self._add(self._datasets, dataset)
        self.add_location(dataset.location)

    def add_dataset_symlink(self, dataset_symlink: DatasetSymlinkDTO):
        self._add(self._dataset_symlinks, dataset_symlink)
        self.add_dataset(dataset_symlink.from_dataset)
        self.add_dataset(dataset_symlink.to_dataset)

    def add_job(self, job: JobDTO):
        self._add(self._jobs, job)
        self.add_location(job.location)

    def add_run(self, run: RunDTO):
        self._add(self._runs, run)
        self.add_job(run.job)
        if run.parent_run:
            self.add_run(run.parent_run)
        if run.user:
            self.add_user(run.user)

    def add_operation(self, operation: OperationDTO):
        self._add(self._operations, operation)
        self.add_run(operation.run)

    def add_input(self, input_: InputDTO):
        self._add(self._inputs, input_)
        self.add_operation(input_.operation)
        self.add_dataset(input_.dataset)
        if input_.schema:
            self.add_schema(input_.schema)

    def add_output(self, output: OutputDTO):
        self._add(self._outputs, output)
        self.add_operation(output.operation)
        self.add_dataset(output.dataset)
        if output.schema:
            self.add_schema(output.schema)

    def add_column_lineage(self, lineage: ColumnLineageDTO):
        self._add(self._column_lineage, lineage)
        self.add_dataset(lineage.source_dataset)
        self.add_dataset(lineage.target_dataset)
        self.add_operation(lineage.operation)

    def add_schema(self, schema: SchemaDTO):
        self._add(self._schemas, schema)

    def add_user(self, user: UserDTO):
        self._add(self._users, user)

    def get_location(self, location_key: tuple) -> LocationDTO:
        return self._locations[location_key]

    def get_schema(self, schema_key: tuple) -> SchemaDTO:
        return self._schemas[schema_key]

    def get_user(self, user_key: tuple) -> UserDTO:
        return self._users[user_key]

    def get_dataset(self, dataset_key: tuple) -> DatasetDTO:
        dataset = self._datasets[dataset_key]
        dataset.location = self.get_location(dataset.location.unique_key)
        return dataset

    def get_dataset_symlink(self, dataset_symlink_key: tuple) -> DatasetSymlinkDTO:
        dataset_symlink = self._dataset_symlinks[dataset_symlink_key]
        dataset_symlink.from_dataset = self.get_dataset(dataset_symlink.from_dataset.unique_key)
        dataset_symlink.to_dataset = self.get_dataset(dataset_symlink.to_dataset.unique_key)
        return dataset_symlink

    def get_job(self, job_key: tuple) -> JobDTO:
        job = self._jobs[job_key]
        job.location = self.get_location(job.location.unique_key)
        return job

    def get_run(self, run_key: tuple) -> RunDTO:
        run = self._runs[run_key]
        run.job = self.get_job(run.job.unique_key)
        if run.parent_run:
            run.parent_run = self.get_run(run.parent_run.unique_key)
        if run.user:
            run.user = self.get_user(run.user.unique_key)
        return run

    def get_operation(self, operation_key: tuple) -> OperationDTO:
        operation = self._operations[operation_key]
        operation.run = self.get_run(operation.run.unique_key)
        return operation

    def get_input(self, input_key: tuple) -> InputDTO:
        input_ = self._inputs[input_key]
        input_.operation = self.get_operation(input_.operation.unique_key)
        input_.dataset = self.get_dataset(input_.dataset.unique_key)
        if input_.schema:
            input_.schema = self.get_schema(input_.schema.unique_key)
        return input_

    def get_output(self, output_key: tuple) -> OutputDTO:
        output = self._outputs[output_key]
        output.operation = self.get_operation(output.operation.unique_key)
        output.dataset = self.get_dataset(output.dataset.unique_key)
        if output.schema:
            output.schema = self.get_schema(output.schema.unique_key)
        return output

    def get_column_lineage(self, output_key: tuple) -> ColumnLineageDTO:
        lineage = self._column_lineage[output_key]
        lineage.operation = self.get_operation(lineage.operation.unique_key)
        lineage.source_dataset = self.get_dataset(lineage.source_dataset.unique_key)
        lineage.target_dataset = self.get_dataset(lineage.target_dataset.unique_key)
        return lineage

    def locations(self) -> list[LocationDTO]:
        return list(map(self.get_location, self._locations))

    def datasets(self) -> list[DatasetDTO]:
        return list(map(self.get_dataset, self._datasets))

    def dataset_symlinks(self) -> list[DatasetSymlinkDTO]:
        return list(map(self.get_dataset_symlink, self._dataset_symlinks))

    def jobs(self) -> list[JobDTO]:
        return list(map(self.get_job, self._jobs))

    def runs(self) -> list[RunDTO]:
        return list(map(self.get_run, self._runs))

    def operations(self) -> list[OperationDTO]:
        return list(map(self.get_operation, self._operations))

    def inputs(self) -> list[InputDTO]:
        return list(map(self.get_input, self._inputs))

    def outputs(self) -> list[OutputDTO]:
        return list(map(self.get_output, self._outputs))

    def column_lineage(self) -> list[ColumnLineageDTO]:
        return list(map(self.get_column_lineage, self._column_lineage))

    def schemas(self) -> list[SchemaDTO]:
        return list(map(self.get_schema, self._schemas))

    def users(self) -> list[UserDTO]:
        return list(map(self.get_user, self._users))


def extract_batch(events: list[OpenLineageRunEvent]) -> BatchExtractionResult:
    result = BatchExtractionResult()
    dataset_cache: dict[tuple[str, str], DatasetDTO] = {}

    for event in events:
        if event.job.facets.jobType and event.job.facets.jobType.jobType == OpenLineageJobType.JOB:
            operation = extract_operation(event)
            result.add_operation(operation)

            for input_dataset in event.inputs:
                input_dto, symlink_dtos = extract_input(operation, input_dataset)

                result.add_input(input_dto)
                dataset_dto_cache_key = (input_dataset.namespace, input_dataset.name)
                dataset_cache[dataset_dto_cache_key] = result.get_dataset(input_dto.dataset.unique_key)

                for symlink_dto in symlink_dtos:
                    result.add_dataset_symlink(symlink_dto)

            for output_dataset in event.outputs:
                output_dto, symlink_dtos = extract_output(operation, output_dataset)

                result.add_output(output_dto)
                dataset_dto_cache_key = (output_dataset.namespace, output_dataset.name)
                dataset_cache[dataset_dto_cache_key] = result.get_dataset(output_dto.dataset.unique_key)

                for symlink_dto in symlink_dtos:
                    result.add_dataset_symlink(symlink_dto)

            for dataset in event.inputs + event.outputs:
                column_lineage = extract_column_lineage(operation, dataset, dataset_cache)
                for item in column_lineage:
                    result.add_column_lineage(item)

        else:
            run = extract_run(event)
            result.add_run(run)

    return result
