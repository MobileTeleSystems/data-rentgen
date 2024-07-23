# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    PageResponseV1,
    RunResponseV1,
    RunsByIdQueryV1,
    RunsByJobQueryV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/runs", tags=["Runs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("/by_id", summary="Paginated list of Runs by id")
async def runs_by_id(
    pagination_args: Annotated[RunsByIdQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
):
    pagination = await unit_of_work.run.pagination_by_id(**pagination_args.model_dump())
    return PageResponseV1[RunResponseV1].from_pagination(pagination)


@router.get("/by_job_id", summary="Paginated list of Runs by Job id")
async def runs_by_job_id(
    pagination_args: Annotated[RunsByJobQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
):
    pagination = await unit_of_work.run.pagination_by_job_id(**pagination_args.model_dump())
    return PageResponseV1[RunResponseV1].from_pagination(pagination)
