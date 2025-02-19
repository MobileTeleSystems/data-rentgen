# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from uuid6 import UUID

from data_rentgen.db.models.dataset_symlink import DatasetSymlink
from data_rentgen.db.models.input import Input
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.output import Output
from data_rentgen.db.models.run import Run
from data_rentgen.server.schemas.v1 import (
    DatasetResponseV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageInputRelationV1,
    LineageNodesResponseV1,
    LineageOutputRelationV1,
    LineageParentRelationV1,
    LineageResponseV1,
    LineageSymlinkRelationV1,
    OperationResponseV1,
    RunResponseV1,
)
from data_rentgen.server.schemas.v1.lineage import (
    LineageIORelationSchemaV1,
    LineageRelationsResponseV1,
)
from data_rentgen.server.services.lineage import LineageServiceResult


async def build_lineage_response(lineage: LineageServiceResult) -> LineageResponseV1:
    datasets = {str(dataset.id): DatasetResponseV1.model_validate(dataset) for dataset in lineage.datasets.values()}
    jobs = {str(job.id): JobResponseV1.model_validate(job) for job in lineage.jobs.values()}
    runs = {run.id: RunResponseV1.model_validate(run) for run in lineage.runs.values()}
    operations = {op.id: OperationResponseV1.model_validate(op) for op in lineage.operations.values()}

    return LineageResponseV1(
        nodes=LineageNodesResponseV1(
            jobs=jobs,
            datasets=datasets,
            runs=runs,  # type: ignore[assignment]
            operations=operations,  # type: ignore[assignment]
        ),
        relations=LineageRelationsResponseV1(
            parents=_get_run_parent_relations(lineage.runs) + _get_operation_parent_relations(lineage.operations),
            symlinks=_get_symlink_relations(lineage.dataset_symlinks),
            inputs=_get_input_relations(lineage.inputs),
            outputs=_get_output_relations(lineage.outputs),
        ),
    )


def _get_run_parent_relations(runs: dict[UUID, Run]) -> list[LineageParentRelationV1]:
    parents = []
    for run_id in sorted(runs):
        run = runs[run_id]
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(run.job_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
        )
        parents.append(relation)
    return parents


def _get_operation_parent_relations(operations: dict[UUID, Operation]) -> list[LineageParentRelationV1]:
    parents = []
    for operation_id in sorted(operations):
        operation = operations[operation_id]
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
        )
        parents.append(relation)
    return parents


def _get_symlink_relations(dataset_symlinks: dict[Any, DatasetSymlink]) -> list[LineageSymlinkRelationV1]:
    symlinks = []
    for key in sorted(dataset_symlinks):
        dataset_symlink = dataset_symlinks[key]
        relation = LineageSymlinkRelationV1(
            type=dataset_symlink.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(dataset_symlink.from_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(dataset_symlink.to_dataset_id)),
        )
        symlinks.append(relation)
    return symlinks


def _get_input_relations(inputs: dict[Any, Input]) -> list[LineageInputRelationV1]:
    relations = []
    for input in inputs.values():
        if input.operation_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input.operation_id)
        elif input.run_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=input.run_id)
        elif input.job_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(input.job_id))

        relation = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(input.dataset_id)),
            to=to,
            last_interaction_at=input.created_at,
            num_bytes=input.num_bytes,
            num_rows=input.num_rows,
            num_files=input.num_files,
            i_schema=LineageIORelationSchemaV1.model_validate(input.schema) if input.schema else None,
        )
        relations.append(relation)

    return sorted(relations, key=lambda x: (str(x.from_.id), str(x.to.id)))


def _get_output_relations(outputs: dict[Any, Output]) -> list[LineageOutputRelationV1]:
    relations = []
    for output in outputs.values():
        if output.operation_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id)
        elif output.run_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=output.run_id)
        elif output.job_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(output.job_id))

        relation = LineageOutputRelationV1(
            type=output.type,
            from_=from_,
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(output.dataset_id)),
            last_interaction_at=output.created_at,
            num_bytes=output.num_bytes,
            num_rows=output.num_rows,
            num_files=output.num_files,
            o_schema=LineageIORelationSchemaV1.model_validate(output.schema) if output.schema else None,
        )
        relations.append(relation)

    return sorted(relations, key=lambda x: (str(x.from_.id), str(x.to.id), x.type))
