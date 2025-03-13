# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends

from data_rentgen.db.models.user import User
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.server.services import get_user

router = APIRouter(
    prefix="/users",
    tags=["User"],
    responses=get_error_responses(),
)


@router.get("/me")
async def check_auth(
    current_user: Annotated[User, Depends(get_user())],
) -> UserResponseV1:
    return UserResponseV1.model_validate(current_user)
