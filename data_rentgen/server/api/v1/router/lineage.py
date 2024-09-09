# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.dto.interaction import InteractionTypeDTO
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    DatasetResponseV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageQueryV1,
    LineageRelationKindV1,
    LineageRelationv1,
    LineageResponseV1,
    OperationResponseV1,
    RunResponseV1,
)
from data_rentgen.server.services import LineageService

router = APIRouter(prefix="/lineage", tags=["Lineage"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Lineage graph")
async def get_lineage(
    pagination_args: Annotated[LineageQueryV1, Depends()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    match pagination_args.point_kind:
        case LineageEntityKindV1.OPERATION:
            get_lineage_method = lineage_service.get_lineage_by_operations  # type: ignore[assignment]
        case LineageEntityKindV1.DATASET:
            get_lineage_method = lineage_service.get_lineage_by_datasets  # type: ignore[assignment]
        case LineageEntityKindV1.RUN:
            get_lineage_method = lineage_service.get_lineage_by_runs  # type: ignore[assignment]
        case LineageEntityKindV1.JOB:
            get_lineage_method = lineage_service.get_lineage_by_jobs  # type: ignore[assignment]

    lineage = await get_lineage_method(
        point_ids=[pagination_args.point_id],  # type: ignore[list-item]
        direction=pagination_args.direction,
        since=pagination_args.since,
        until=pagination_args.until,
        depth=pagination_args.depth,
    )

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

    for interaction in lineage.interactions:
        operation_item = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=interaction.operation_id)
        dataset_item = LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=interaction.dataset_id)

        if interaction.type == InteractionTypeDTO.READ:
            relation = LineageRelationv1(
                kind=LineageRelationKindV1.INTERACTION,
                type=interaction.type,
                from_=dataset_item,
                to=operation_item,
            )
        else:
            relation = LineageRelationv1(
                kind=LineageRelationKindV1.INTERACTION,
                type=interaction.type,
                from_=operation_item,
                to=dataset_item,
            )

        response.relations.append(relation)

    return response
