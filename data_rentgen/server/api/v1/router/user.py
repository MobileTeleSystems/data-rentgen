# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas.not_authorized import NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    UserResponseV1,
)
from data_rentgen.server.services import get_user

router = APIRouter(
    prefix="/users",
    tags=["User"],
    responses=get_error_responses(include={NotAuthorizedSchema}),
)


@router.get("/me", summary="Get current user info")
async def check_auth(
    current_user: Annotated[User, Depends(get_user())],
) -> UserResponseV1:
    return UserResponseV1.model_validate(current_user)
