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
)
from data_rentgen.server.services import LineageService
from data_rentgen.server.utils.lineage_response import build_lineage_response
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/jobs", tags=["Jobs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Jobs")
async def paginate_jobs(
    query_args: Annotated[JobPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[JobResponseV1]:
    pagination = await unit_of_work.job.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        job_ids=query_args.job_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[JobResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Job lineage graph")
async def get_jobs_lineage(
    query_args: Annotated[JobLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_jobs(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        # TODO: add pagination args in DOP-20060
        granularity="OPERATION",
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
    )

    return await build_lineage_response(lineage)
