# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm

from data_rentgen.dependencies import Stub
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import NotAuthorizedSchema
from data_rentgen.server.providers.auth import AuthProvider, DummyAuthProvider
from data_rentgen.server.schemas.v1.auth import AuthTokenSchema

router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
    responses=get_error_responses(include={NotAuthorizedSchema, InvalidRequestSchema}),
)


@router.post("/token")
async def token(
    auth_provider: Annotated[DummyAuthProvider, Depends(Stub(AuthProvider))],
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> AuthTokenSchema:
    user_token = await auth_provider.get_token_password_grant(
        login=form_data.username,
    )
    return AuthTokenSchema.model_validate(user_token)
