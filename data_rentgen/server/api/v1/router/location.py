# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotFoundSchema
from data_rentgen.server.schemas.v1 import (
    LocationDetailedResponseV1,
    LocationPaginateQueryV1,
    PageResponseV1,
    UpdateLocationRequestV1,
)
from data_rentgen.server.services import LocationService, get_user

router = APIRouter(
    prefix="/locations",
    tags=["Locations"],
    responses=get_error_responses(include={InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Locations")
async def paginate_locations(
    query_args: Annotated[LocationPaginateQueryV1, Depends()],
    location_service: Annotated[LocationService, Depends()],
    current_user: User = Depends(get_user()),
) -> PageResponseV1[LocationDetailedResponseV1]:
    pagination = await location_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        location_ids=query_args.location_id,
        location_type=query_args.location_type,
        search_query=query_args.search_query,
    )
    return PageResponseV1[LocationDetailedResponseV1].from_pagination(pagination)


@router.patch(
    "/{location_id}",
    responses=get_error_responses(include={InvalidRequestSchema, NotFoundSchema}),
)
async def update_location(
    location_id: int,
    location_data: UpdateLocationRequestV1,
    location_service: Annotated[LocationService, Depends()],
    current_user: User = Depends(get_user()),
) -> LocationDetailedResponseV1:
    location = await location_service.update_external_id(location_id, location_data.external_id)
    return LocationDetailedResponseV1.model_validate(location)
