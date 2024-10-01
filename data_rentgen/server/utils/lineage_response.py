# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.server.schemas.v1 import (
    DatasetResponseV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageRelationKindV1,
    LineageRelationV1,
    LineageResponseV1,
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
            LineageRelationV1(
                kind=LineageRelationKindV1.PARENT,
                from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=run.job_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
            ),
        )
        response.nodes.append(RunResponseV1.model_validate(run))

    for operation_id in sorted(lineage.operations):
        operation = lineage.operations[operation_id]
        response.relations.append(
            LineageRelationV1(
                kind=LineageRelationKindV1.PARENT,
                from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
            ),
        )
        response.nodes.append(OperationResponseV1.model_validate(operation))

    for symlink_id in sorted(lineage.dataset_symlinks):
        dataset_symlink = lineage.dataset_symlinks[symlink_id]
        relation = LineageRelationV1(
            kind=LineageRelationKindV1.SYMLINK,
            type=dataset_symlink.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.from_dataset_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.to_dataset_id),
        )
        response.relations.append(relation)

    input_relations = await _add_input_relations(lineage.inputs)
    response.relations.extend(input_relations)

    output_relations = await _add_output_relations(lineage.outputs)
    response.relations.extend(output_relations)

    return response


async def _add_input_relations(
    inputs: list,
) -> list[LineageRelationV1]:
    relations = []
    for input in inputs:
        if input.operation_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input.operation_id)
        elif input.run_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=input.run_id)
        elif input.job_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=input.job_id)
        relation = LineageRelationV1(
            kind=LineageRelationKindV1.INPUT,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=input.dataset_id),
            to=to,
        )
        relations.append(relation)
    return sorted(relations, key=lambda x: (x.from_.id, x.to.id, x.type))


async def _add_output_relations(
    outputs: list,
) -> list[LineageRelationV1]:
    relations = []
    for output in outputs:
        if output.operation_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id)
        elif output.run_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=output.run_id)
        elif output.job_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=output.job_id)
        relation = LineageRelationV1(
            kind=LineageRelationKindV1.OUTPUT,
            type=output.type,
            from_=from_,
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=output.dataset_id),
        )
        relations.append(relation)
    return sorted(relations, key=lambda x: (x.from_.id, x.to.id, x.type))
