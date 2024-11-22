# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TypeVar

from data_rentgen.consumer.extractors.input import extract_input
from data_rentgen.consumer.extractors.operation import extract_operation
from data_rentgen.consumer.extractors.output import extract_output
from data_rentgen.consumer.extractors.run import extract_run
from data_rentgen.consumer.openlineage.job_facets.job_type import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.dto import (
    DatasetDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
    SchemaDTO,
    UserDTO,
)
from data_rentgen.dto.dataset_symlink import DatasetSymlinkDTO

T = TypeVar(
    "T",
    LocationDTO,
    DatasetDTO,
    DatasetSymlinkDTO,
    JobDTO,
    RunDTO,
    OperationDTO,
    InputDTO,
    OutputDTO,
    SchemaDTO,
    UserDTO,
)


class BatchExtractionResult:  # noqa: WPS338, WPS214
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
        self._schemas: dict[tuple, SchemaDTO] = {}
        self._users: dict[tuple, UserDTO] = {}

    def __repr__(self):
        return (
            "ExtractionResult("  # noqa: WPS237
            f"locations={len(self._locations)}, "
            f"datasets={len(self._datasets)}, "
            f"dataset_symlinks={len(self._dataset_symlinks)}, "
            f"jobs={len(self._jobs)}, "
            f"runs={len(self._runs)}, "
            f"operations={len(self._operations)}, "
            f"inputs={len(self._inputs)}, "
            f"outputs={len(self._outputs)}, "
            f"schemas={len(self._schemas)}, "
            f"users={len(self._users)}"
            ")"
        )

    @staticmethod
    def _add(context: dict[tuple, T], new_item: T) -> dict[tuple, T]:  # noqa: WPS602
        if new_item.unique_key in context:
            context[new_item.unique_key] = context[new_item.unique_key].merge(new_item)
        else:
            context[new_item.unique_key] = new_item
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

    def add_input(self, input: InputDTO):
        self._add(self._inputs, input)
        self.add_operation(input.operation)
        self.add_dataset(input.dataset)
        if input.schema:
            self.add_schema(input.schema)

    def add_output(self, output: OutputDTO):
        self._add(self._outputs, output)
        self.add_operation(output.operation)
        self.add_dataset(output.dataset)
        if output.schema:
            self.add_schema(output.schema)

    def add_schema(self, schema: SchemaDTO):
        self._add(self._schemas, schema)

    def add_user(self, user: UserDTO):
        self._add(self._users, user)

    def _get_location(self, location_key: tuple) -> LocationDTO:
        return self._locations[location_key]

    def _get_schema(self, schema_key: tuple) -> SchemaDTO:
        return self._schemas[schema_key]

    def _get_user(self, user_key: tuple) -> UserDTO:
        return self._users[user_key]

    def _get_dataset(self, dataset_key: tuple) -> DatasetDTO:
        dataset = self._datasets[dataset_key]
        dataset.location = self._get_location(dataset.location.unique_key)
        return dataset

    def _get_dataset_symlink(self, dataset_symlink_key: tuple) -> DatasetSymlinkDTO:
        dataset_symlink = self._dataset_symlinks[dataset_symlink_key]
        dataset_symlink.from_dataset = self._get_dataset(dataset_symlink.from_dataset.unique_key)
        dataset_symlink.to_dataset = self._get_dataset(dataset_symlink.to_dataset.unique_key)
        return dataset_symlink

    def _get_job(self, job_key: tuple) -> JobDTO:
        job = self._jobs[job_key]
        job.location = self._get_location(job.location.unique_key)
        return job

    def _get_run(self, run_key: tuple) -> RunDTO:
        run = self._runs[run_key]
        run.job = self._get_job(run.job.unique_key)
        if run.parent_run:
            run.parent_run = self._get_run(run.parent_run.unique_key)
        if run.user:
            run.user = self._get_user(run.user.unique_key)
        return run

    def _get_operation(self, operation_key: tuple) -> OperationDTO:
        operation = self._operations[operation_key]
        operation.run = self._get_run(operation.run.unique_key)
        return operation

    def _get_input(self, input_key: tuple) -> InputDTO:
        input = self._inputs[input_key]
        input.operation = self._get_operation(input.operation.unique_key)
        input.dataset = self._get_dataset(input.dataset.unique_key)
        if input.schema:
            input.schema = self._get_schema(input.schema.unique_key)
        return input

    def _get_output(self, output_key: tuple) -> OutputDTO:
        output = self._outputs[output_key]
        output.operation = self._get_operation(output.operation.unique_key)
        output.dataset = self._get_dataset(output.dataset.unique_key)
        if output.schema:
            output.schema = self._get_schema(output.schema.unique_key)
        return output

    def locations(self) -> list[LocationDTO]:
        return list(map(self._get_location, self._locations))

    def datasets(self) -> list[DatasetDTO]:
        return list(map(self._get_dataset, self._datasets))

    def dataset_symlinks(self) -> list[DatasetSymlinkDTO]:
        return list(map(self._get_dataset_symlink, self._dataset_symlinks))

    def jobs(self) -> list[JobDTO]:
        return list(map(self._get_job, self._jobs))

    def runs(self) -> list[RunDTO]:
        return list(map(self._get_run, self._runs))

    def operations(self) -> list[OperationDTO]:
        return list(map(self._get_operation, self._operations))

    def inputs(self) -> list[InputDTO]:
        return list(map(self._get_input, self._inputs))

    def outputs(self) -> list[OutputDTO]:
        return list(map(self._get_output, self._outputs))

    def schemas(self) -> list[SchemaDTO]:
        return list(map(self._get_schema, self._schemas))

    def users(self) -> list[UserDTO]:
        return list(map(self._get_user, self._users))


def extract_batch(events: list[OpenLineageRunEvent]) -> BatchExtractionResult:
    result = BatchExtractionResult()

    for event in events:
        if event.job.facets.jobType and event.job.facets.jobType.jobType == OpenLineageJobType.JOB:
            operation = extract_operation(event)
            result.add_operation(operation)
            for input_dataset in event.inputs:
                input, symlinks = extract_input(operation, input_dataset)
                result.add_input(input)
                for symlink in symlinks:
                    result.add_dataset_symlink(symlink)

            for output_dataset in event.outputs:
                output, symlinks = extract_output(operation, output_dataset)
                result.add_output(output)
                for symlink in symlinks:  # noqa: WPS440
                    result.add_dataset_symlink(symlink)

        else:
            run = extract_run(event)
            result.add_run(run)

    return result
