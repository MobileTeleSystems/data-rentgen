# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from logging import getLogger

from fastapi import APIRouter, Depends

from data_rentgen.db.models.user import User
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import (
    NotAuthorizedRedirectSchema,
    NotAuthorizedSchema,
)
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.server.services import get_user

logger = getLogger(__name__)


router = APIRouter(
    prefix="/users",
    tags=["User"],
    responses=get_error_responses(include={NotAuthorizedSchema, InvalidRequestSchema, NotAuthorizedRedirectSchema}),
)


@router.get("/me")
async def check_auth(
    current_user: User = Depends(get_user()),
):
    logger.info("User check: %s", current_user.name)
    return UserResponseV1.model_validate(current_user)
