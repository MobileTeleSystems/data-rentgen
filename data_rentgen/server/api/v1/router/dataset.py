# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    DatasetLineageQueryV1,
    DatasetPaginateQueryV1,
    DatasetResponseV1,
    LineageResponseV1,
    PageResponseV1,
    SearchPaginateQueryV1,
)
from data_rentgen.server.services import LineageService
from data_rentgen.server.utils.lineage_response import build_lineage_response
from data_rentgen.services import UnitOfWork

router = APIRouter(prefix="/datasets", tags=["Datasets"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Datasets")
async def paginate_datasets(
    pagination_args: Annotated[DatasetPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[DatasetResponseV1]:
    pagination = await unit_of_work.dataset.paginate(
        page=pagination_args.page,
        page_size=pagination_args.page_size,
        dataset_ids=pagination_args.dataset_id,
    )
    return PageResponseV1[DatasetResponseV1].from_pagination(pagination)


@router.get("/search", summary="Search Datasets")
async def search_datasets(
    pagination_args: Annotated[SearchPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[DatasetResponseV1]:
    pagination = await unit_of_work.dataset.search(
        page=pagination_args.page,
        page_size=pagination_args.page_size,
        search_query=pagination_args.search_query,
    )
    return PageResponseV1[DatasetResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Dataset lineage graph")
async def get_dataset_lineage(
    pagination_args: Annotated[DatasetLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_datasets(
        start_node_ids=[pagination_args.start_node_id],  # type: ignore[list-item]
        direction=pagination_args.direction,
        granularity=pagination_args.granularity,
        since=pagination_args.since,
        until=pagination_args.until,
        depth=pagination_args.depth,
    )

    return await build_lineage_response(lineage)
