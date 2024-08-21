# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    OperationQueryV1,
    OperationResponseV1,
    PageResponseV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(
    prefix="/operations",
    tags=["Operations"],
    responses=get_error_responses(include={InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Operations")
async def operations(
    pagination_args: Annotated[OperationQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[OperationResponseV1]:
    if pagination_args.operation_id:
        pagination = await unit_of_work.operation.pagination_by_id(
            page=pagination_args.page,
            page_size=pagination_args.page_size,
            operation_ids=pagination_args.operation_id,
        )
    else:
        pagination = await unit_of_work.operation.pagination_by_run_id(
            page=pagination_args.page,
            page_size=pagination_args.page_size,
            run_id=pagination_args.run_id,  # type: ignore[arg-type]
            since=pagination_args.since,  # type: ignore[arg-type]
            until=pagination_args.until,
        )
    return PageResponseV1[OperationResponseV1].from_pagination(pagination)
