# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    DatasetPaginateQueryV1,
    DatasetResponseV1,
    PageResponseV1,
    SearchPaginateQueryV1,
)
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


@router.get("/search", summary="Search Datasets by name")
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
