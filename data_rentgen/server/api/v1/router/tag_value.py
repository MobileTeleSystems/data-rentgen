# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models.user import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    PageResponseV1,
    TagValueDetailedResponseV1,
    TagValuePaginateQueryV1,
)
from data_rentgen.server.services import TagValueService, get_user

router = APIRouter(
    prefix="/tag-values",
    tags=["TagValues"],
    responses=get_error_responses(include={NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of TagValues")
async def paginate_tag_values(
    query_args: Annotated[TagValuePaginateQueryV1, Query()],
    tag_value_service: Annotated[TagValueService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[TagValueDetailedResponseV1]:
    pagination = await tag_value_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        tag_id=query_args.tag_id,
        tag_value_ids=query_args.tag_value_id,
        search_query=query_args.search_query,
    )
    return PageResponseV1[TagValueDetailedResponseV1].from_pagination(pagination)
