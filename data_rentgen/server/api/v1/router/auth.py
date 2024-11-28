# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm

from data_rentgen.dependencies import Stub
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import NotAuthorizedSchema
from data_rentgen.server.providers.auth import (
    AuthProvider,
    DummyAuthProvider,
    KeycloakAuthProvider,
)
from data_rentgen.server.schemas.v1.auth import AuthTokenSchema
from data_rentgen.server.utils.state import validate_state

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


@router.get("/callback")
async def auth_callback(
    request: Request,
    code: str,
    state: str,
    auth_provider: Annotated[KeycloakAuthProvider, Depends(Stub(AuthProvider))],
):
    original_redirect_url = validate_state(state)
    if not original_redirect_url:
        raise HTTPException(status_code=400, detail="Invalid state parameter")  # noqa: WPS432
    user_token = await auth_provider.get_token_authorization_code_grant(
        code=code,
        redirect_uri=auth_provider.settings.keycloak.redirect_uri,
    )
    request.session["access_token"] = user_token["access_token"]
    request.session["refresh_token"] = user_token["refresh_token"]

    return RedirectResponse(url=original_redirect_url)
