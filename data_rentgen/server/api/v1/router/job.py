# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    JobDetailedResponseV1,
    JobLineageQueryV1,
    JobPaginateQueryV1,
    LineageResponseV1,
    PageResponseV1,
)
from data_rentgen.server.services import JobService, LineageService, get_user
from data_rentgen.server.utils.lineage_response import build_lineage_response

router = APIRouter(prefix="/jobs", tags=["Jobs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Jobs")
async def paginate_jobs(
    query_args: Annotated[JobPaginateQueryV1, Depends()],
    job_service: Annotated[JobService, Depends()],
    current_user: User = Depends(get_user()),
) -> PageResponseV1[JobDetailedResponseV1]:
    pagination = await job_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        job_ids=query_args.job_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[JobDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Job lineage graph")
async def get_jobs_lineage(
    query_args: Annotated[JobLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: User = Depends(get_user()),
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_jobs(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
    )

    return await build_lineage_response(lineage)
