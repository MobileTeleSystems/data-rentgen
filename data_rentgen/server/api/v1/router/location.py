# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.commons.errors import get_error_responses
from data_rentgen.commons.errors.schemas import InvalidRequestSchema, NotFoundSchema
from data_rentgen.server.schemas.v1 import (
    LocationPaginateQueryV1,
    LocationResponseV1,
    PageResponseV1,
    UpdateLocationRequestV1,
)
from data_rentgen.services import UnitOfWork

router = APIRouter(
    prefix="/locations",
    tags=["Locations"],
    responses=get_error_responses(include={InvalidRequestSchema, NotFoundSchema}),
)


@router.get("", summary="Paginated list of Locations")
async def paginate_locations(
    query_args: Annotated[LocationPaginateQueryV1, Depends()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> PageResponseV1[LocationResponseV1]:
    pagination = await unit_of_work.location.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        location_ids=query_args.location_id,
        location_type=query_args.location_type,
        search_query=query_args.search_query,
    )
    return PageResponseV1[LocationResponseV1].from_pagination(pagination)


@router.patch("/{location_id}")
async def update_location(
    location_id: int,
    location_data: UpdateLocationRequestV1,
    unit_of_work: Annotated[UnitOfWork, Depends()],
) -> LocationResponseV1:
    location = await unit_of_work.location.update_external_id(location_id, location_data.external_id)
    return LocationResponseV1.model_validate(location)
