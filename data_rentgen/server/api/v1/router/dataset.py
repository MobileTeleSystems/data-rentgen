# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    DatasetDetailedResponseV1,
    DatasetLineageQueryV1,
    DatasetPaginateQueryV1,
    LineageResponseV1,
    PageResponseV1,
)
from data_rentgen.server.services import DatasetService, LineageService, get_user
from data_rentgen.server.utils.lineage_response import build_lineage_response

router = APIRouter(prefix="/datasets", tags=["Datasets"], responses=get_error_responses(include={InvalidRequestSchema}))


@router.get("", summary="Paginated list of Datasets")
async def paginate_datasets(
    query_args: Annotated[DatasetPaginateQueryV1, Depends()],
    dataset_service: Annotated[DatasetService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[DatasetDetailedResponseV1]:
    pagination = await dataset_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        dataset_ids=query_args.dataset_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[DatasetDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Dataset lineage graph")
async def get_datasets_lineage(
    query_args: Annotated[DatasetLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_datasets(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
        column_lineage=query_args.column_lineage,
    )

    return await build_lineage_response(lineage)
