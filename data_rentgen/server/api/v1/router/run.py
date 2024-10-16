# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    LineageResponseV1,
    PageResponseV1,
    RunLineageQueryV1,
    RunResponseV1,
    RunsQueryV1,
)
from data_rentgen.server.services import LineageService
from data_rentgen.server.utils.lineage_response import build_lineage_response
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/runs", tags=["Runs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Runs")
async def runs(
    query_args: Annotated[RunsQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[RunResponseV1]:
    pagination = await unit_of_work.run.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        since=query_args.since,
        until=query_args.until,
        run_ids=query_args.run_id,
        job_id=query_args.job_id,
        parent_run_id=query_args.parent_run_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[RunResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Run lineage graph")
async def get_runs_lineage(
    query_args: Annotated[RunLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_runs(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
    )
    return await build_lineage_response(lineage)
