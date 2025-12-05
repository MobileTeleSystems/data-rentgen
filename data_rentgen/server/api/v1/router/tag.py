# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models.user import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    PageResponseV1,
    TagDetailedResponseV1,
)
from data_rentgen.server.schemas.v1.tag import TagPaginateQueryV1
from data_rentgen.server.services import get_user
from data_rentgen.server.services.tag import TagService

router = APIRouter(
    prefix="/tags",
    tags=["Tags"],
    responses=get_error_responses(include={NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Tags")
async def paginate_tags(
    query_args: Annotated[TagPaginateQueryV1, Query()],
    tag_service: Annotated[TagService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[TagDetailedResponseV1]:
    pagination = await tag_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        tag_ids=query_args.tag_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[TagDetailedResponseV1].from_pagination(pagination)
