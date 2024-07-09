# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.schemas.v1 import (
    JobPaginateQueryV1,
    JobResponseV1,
    PageResponseV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/job", tags=["Jobs"], responses=get_error_responses())


@router.get("", summary="Paginated list of Jobs")
async def paginate_jobs(
    pagination_args: Annotated[JobPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[JobResponseV1]:
    pagination = await unit_of_work.job.paginate(**pagination_args.model_dump())
    return PageResponseV1[JobResponseV1].from_pagination(pagination)
