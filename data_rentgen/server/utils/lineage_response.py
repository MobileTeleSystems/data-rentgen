# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable

from data_rentgen.server.schemas.v1 import (
    DatasetResponseV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageInputRelationV1,
    LineageOutputRelationV1,
    LineageParentRelationV1,
    LineageResponseV1,
    LineageSymlinkRelationV1,
    OperationResponseV1,
    RunResponseV1,
)
from data_rentgen.server.services.lineage import LineageServiceResult


async def build_lineage_response(lineage: LineageServiceResult) -> LineageResponseV1:
    response = LineageResponseV1()
    for job_id in sorted(lineage.jobs):
        job = lineage.jobs[job_id]
        response.nodes.append(JobResponseV1.model_validate(job))

    for dataset_id in sorted(lineage.datasets):
        dataset = lineage.datasets[dataset_id]
        response.nodes.append(DatasetResponseV1.model_validate(dataset))

    for run_id in sorted(lineage.runs):
        run = lineage.runs[run_id]
        response.relations.append(
            LineageParentRelationV1(
                from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=run.job_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
            ),
        )
        response.nodes.append(RunResponseV1.model_validate(run))

    for operation_id in sorted(lineage.operations):
        operation = lineage.operations[operation_id]
        response.relations.append(
            LineageParentRelationV1(
                from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
            ),
        )
        response.nodes.append(OperationResponseV1.model_validate(operation))

    for symlink_id in sorted(lineage.dataset_symlinks):
        dataset_symlink = lineage.dataset_symlinks[symlink_id]
        relation = LineageSymlinkRelationV1(
            type=dataset_symlink.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.from_dataset_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.to_dataset_id),
        )
        response.relations.append(relation)

    input_relations = await _add_input_relations(lineage.inputs.values())
    response.relations.extend(input_relations)

    output_relations = await _add_output_relations(lineage.outputs.values())
    response.relations.extend(output_relations)

    return response


async def _add_input_relations(
    inputs: Iterable,
) -> list[LineageInputRelationV1]:
    relations = []
    for input in inputs:
        if input.operation_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input.operation_id)
        elif input.run_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=input.run_id)
        elif input.job_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=input.job_id)
        relation = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=input.dataset_id),
            to=to,
            last_interaction_at=input.created_at,
            num_bytes=input.num_bytes,
            num_rows=input.num_rows,
            num_files=input.num_files,
        )
        relations.append(relation)
    return sorted(relations, key=lambda x: (x.from_.id, x.to.id))


async def _add_output_relations(
    outputs: Iterable,
) -> list[LineageOutputRelationV1]:
    relations = []
    for output in outputs:
        if output.operation_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id)
        elif output.run_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=output.run_id)
        elif output.job_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=output.job_id)
        relation = LineageOutputRelationV1(
            type=output.type,
            from_=from_,
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=output.dataset_id),
            last_interaction_at=output.created_at,
            num_bytes=output.num_bytes,
            num_rows=output.num_rows,
            num_files=output.num_files,
        )
        relations.append(relation)
    return sorted(relations, key=lambda x: (x.from_.id, x.to.id, x.type))
