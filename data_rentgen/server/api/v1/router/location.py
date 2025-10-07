# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.errors.schemas.not_found import NotFoundSchema
from data_rentgen.server.schemas.v1 import (
    LocationDetailedResponseV1,
    LocationPaginateQueryV1,
    PageResponseV1,
    UpdateLocationRequestV1,
)
from data_rentgen.server.schemas.v1.location import LocationTypesResponseV1
from data_rentgen.server.services import LocationService, get_user

router = APIRouter(
    prefix="/locations",
    tags=["Locations"],
)


@router.get(
    "",
    summary="Paginated list of Locations",
    responses=get_error_responses(include={NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema}),
)
async def paginate_locations(
    query_args: Annotated[LocationPaginateQueryV1, Query()],
    location_service: Annotated[LocationService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
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
    summary="Update location external_id",
    responses=get_error_responses(
        include={NotFoundSchema, NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema},
    ),
)
async def update_location(
    location_id: int,
    location_data: UpdateLocationRequestV1,
    location_service: Annotated[LocationService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LocationDetailedResponseV1:
    location = await location_service.update_external_id(location_id, location_data.external_id)
    return LocationDetailedResponseV1.model_validate(location)


@router.get("/types", summary="Get distinct types of Locations")
async def get_location_types(
    location_service: Annotated[LocationService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LocationTypesResponseV1:
    location_types = await location_service.get_location_types()
    return LocationTypesResponseV1(location_types=list(location_types))
