# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    OperationByIdQueryV1,
    OperationByRunQueryV1,
    OperationResponseV1,
    PageResponseV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(
    prefix="/operations",
    tags=["Operations"],
    responses=get_error_responses(include={InvalidRequestSchema}),
)


@router.get("/by_id", summary="Paginated list of Operations by id")
async def operations_by_id(
    pagination_args: Annotated[OperationByIdQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[OperationResponseV1]:
    pagination = await unit_of_work.operation.pagination_by_id(**pagination_args.model_dump())
    return PageResponseV1[OperationResponseV1].from_pagination(pagination)


@router.get("/by_run_id", summary="Paginated list of Operation by Run id")
async def operations_by_run_id(
    pagination_args: Annotated[OperationByRunQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[OperationResponseV1]:
    pagination = await unit_of_work.operation.pagination_by_run_id(**pagination_args.model_dump())
    return PageResponseV1[OperationResponseV1].from_pagination(pagination)
