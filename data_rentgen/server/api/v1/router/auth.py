# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import base64
from http import HTTPStatus
from logging import getLogger
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordRequestForm

from data_rentgen.db.models.user import User
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
from data_rentgen.server.schemas.v1.user import UserResponseV1
from data_rentgen.server.services import get_user

logger = getLogger(__name__)


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
    )
    return AuthTokenSchema.model_validate(user_token)


@router.get("/callback")
async def auth_callback(
    request: Request,
    state: str,
    code: str,
    auth_provider: Annotated[KeycloakAuthProvider, Depends(Stub(AuthProvider))],
):
    state = base64.b64decode(state.encode("utf-8"))  # type: ignore[assignment]
    original_url = state.decode("utf-8")  # type: ignore[attr-defined]

    if not original_url:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Invalid original_url")
    code_grant = await auth_provider.get_token_authorization_code_grant(
        code=code,
        redirect_uri=auth_provider.settings.keycloak.redirect_uri,
    )
    request.session["access_token"] = code_grant["access_token"]
    request.session["refresh_token"] = code_grant["refresh_token"]

    return HTTPStatus.OK


@router.get("/me")
async def check_auth(
    current_user: User = Depends(get_user()),
):
    logger.info("User check: %s", current_user.name)
    return UserResponseV1.model_validate(current_user)
