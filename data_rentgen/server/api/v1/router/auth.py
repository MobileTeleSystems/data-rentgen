# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.security import OAuth2PasswordRequestForm

from data_rentgen.dependencies import Stub
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import (
    NotAuthorizedRedirectSchema,
    NotAuthorizedSchema,
)
from data_rentgen.server.providers.auth import (
    AuthProvider,
    DummyAuthProvider,
    KeycloakAuthProvider,
)
from data_rentgen.server.schemas.v1.auth import AuthTokenSchema

router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
    responses=get_error_responses(include={NotAuthorizedSchema, InvalidRequestSchema, NotAuthorizedRedirectSchema}),
)


@router.post("/token")
async def token(
    auth_provider: Annotated[DummyAuthProvider, Depends(Stub(AuthProvider))],
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> AuthTokenSchema:
    user_token = await auth_provider.get_token_password_grant(
        login=form_data.username,
        password=form_data.password,
    )
    return AuthTokenSchema.model_validate(user_token)


@router.get("/callback")
async def auth_callback(
    request: Request,
    code: str,
    auth_provider: Annotated[KeycloakAuthProvider, Depends(Stub(AuthProvider))],
):
    code_grant = await auth_provider.get_token_authorization_code_grant(
        code=code,
    )
    request.session["access_token"] = code_grant["access_token"]
    request.session["refresh_token"] = code_grant["refresh_token"]

    return {}
