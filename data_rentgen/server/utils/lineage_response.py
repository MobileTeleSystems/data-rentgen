# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from data_rentgen.server.schemas.v1 import (
    DatasetResponseV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageRelationKindV1,
    LineageRelationv1,
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
            LineageRelationv1(
                kind=LineageRelationKindV1.PARENT,
                from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=run.job_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
            ),
        )
        response.nodes.append(RunResponseV1.model_validate(run))

    for operation_id in sorted(lineage.operations):
        operation = lineage.operations[operation_id]
        response.relations.append(
            LineageRelationv1(
                kind=LineageRelationKindV1.PARENT,
                from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
                to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
            ),
        )
        response.nodes.append(OperationResponseV1.model_validate(operation))

    for symlink_id in sorted(lineage.dataset_symlinks):
        dataset_symlink = lineage.dataset_symlinks[symlink_id]
        relation = LineageRelationv1(
            kind=LineageRelationKindV1.SYMLINK,
            type=dataset_symlink.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.from_dataset_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=dataset_symlink.to_dataset_id),
        )
        response.relations.append(relation)

    for input in lineage.inputs:
        relation = LineageRelationv1(
            kind=LineageRelationKindV1.INPUT,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=input.dataset_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input.operation_id),
        )
        response.relations.append(relation)

    for output in lineage.outputs:
        relation = LineageRelationv1(
            kind=LineageRelationKindV1.OUTPUT,
            type=output.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=output.dataset_id),
        )
        response.relations.append(relation)

    return response
