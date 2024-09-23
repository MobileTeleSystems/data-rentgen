# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    JobLineageQueryV1,
    JobPaginateQueryV1,
    JobResponseV1,
    LineageResponseV1,
    PageResponseV1,
    SearchPaginateQueryV1,
)
from data_rentgen.server.services import LineageService
from data_rentgen.server.utils.lineage_response import build_lineage_response
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


@router.get("/lineage", summary="Get Jobs lineage graph")
async def get_jobs_lineage(
    pagination_args: Annotated[JobLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_jobs(
        point_ids=[pagination_args.point_id],  # type: ignore[list-item]
        direction=pagination_args.direction,
        since=pagination_args.since,
        until=pagination_args.until,
        depth=pagination_args.depth,
    )

    return await build_lineage_response(lineage)
