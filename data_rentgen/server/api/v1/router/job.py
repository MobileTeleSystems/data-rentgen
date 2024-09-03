# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    JobPaginateQueryV1,
    JobResponseV1,
    PageResponseV1,
    SearchPaginateQueryV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/jobs", tags=["Jobs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Jobs")
async def paginate_jobs(
    pagination_args: Annotated[JobPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[JobResponseV1]:
    pagination = await unit_of_work.job.paginate(
        page=pagination_args.page,
        page_size=pagination_args.page_size,
        job_ids=pagination_args.job_id,
    )
    return PageResponseV1[JobResponseV1].from_pagination(pagination)


@router.get("/search", summary="Search Jobs")
async def search_jobs(
    pagination_args: Annotated[SearchPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[JobResponseV1]:
    pagination = await unit_of_work.job.search(
        page=pagination_args.page,
        page_size=pagination_args.page_size,
        search_query=pagination_args.search_query,
    )

    return PageResponseV1[JobResponseV1].from_pagination(pagination)
