# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    PageResponseV1,
    RunResponseV1,
    RunsQueryV1,
    SearchPaginateQueryV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/runs", tags=["Runs"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Runs")
async def runs(
    pagination_args: Annotated[RunsQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[RunResponseV1]:
    if pagination_args.run_id:
        pagination = await unit_of_work.run.pagination_by_id(
            page=pagination_args.page,
            page_size=pagination_args.page_size,
            run_ids=pagination_args.run_id,
        )
    elif pagination_args.job_id:
        pagination = await unit_of_work.run.pagination_by_job_id(
            page=pagination_args.page,
            page_size=pagination_args.page_size,
            job_id=pagination_args.job_id,  # type: ignore[arg-type]
            since=pagination_args.since,  # type: ignore[arg-type]
            until=pagination_args.until,
        )
    elif pagination_args.parent_run_id:
        pagination = await unit_of_work.run.pagination_by_parent_run_id(
            page=pagination_args.page,
            page_size=pagination_args.page_size,
            parent_run_id=pagination_args.parent_run_id,  # type: ignore[arg-type]
            since=pagination_args.since,  # type: ignore[arg-type]
            until=pagination_args.until,
        )
    return PageResponseV1[RunResponseV1].from_pagination(pagination)


@router.get("/search", summary="Search Runs")
async def search_runss(
    pagination_args: Annotated[SearchPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[RunResponseV1]:
    pagination = await unit_of_work.run.search(
        page=pagination_args.page,
        page_size=pagination_args.page_size,
        search_query=pagination_args.search_query,
    )

    return PageResponseV1[RunResponseV1].from_pagination(pagination)
