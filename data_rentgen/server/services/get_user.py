# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable, Coroutine
from enum import Enum
from typing import Annotated, Any

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer

from data_rentgen.db.models import User
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.server.providers.auth import AuthProvider
from data_rentgen.server.providers.auth.personal_token_provider import PersonalTokenAuthProvider


class PersonalTokenPolicy(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE = "require"


personal_token_schema = HTTPBearer(
    description="Perform authentication using PersonalToken",
    auto_error=False,
)
oauth2_schema = OAuth2PasswordBearer(
    description="Perform authentication using configured AuthProvider",
    tokenUrl="v1/auth/token",
    auto_error=False,
)


async def get_user_via_any_credentials_source(
    request: Request,
    real_auth_provider: Annotated[AuthProvider, Depends()],
    personal_token_provider: Annotated[PersonalTokenAuthProvider, Depends()],
    access_token: Annotated[str | None, Depends(oauth2_schema)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(personal_token_schema)],
) -> User:
    result = await personal_token_provider.get_optional_user(
        access_token=credentials.credentials if credentials else access_token,
        request=request,
    )
    if result:
        return result

    return await real_auth_provider.get_current_user(
        access_token=access_token,
        request=request,
    )


async def get_user_via_personal_token(
    request: Request,
    auth_provider: Annotated[PersonalTokenAuthProvider, Depends()],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(personal_token_schema)],
) -> User:
    result = await auth_provider.get_current_user(
        access_token=credentials.credentials if credentials else None,
        request=request,
    )
    if result:
        return result
    msg = "Missing Authorization header"
    raise AuthorizationError(msg)


async def get_user_via_access_token_or_cookie(
    request: Request,
    auth_provider: Annotated[AuthProvider, Depends()],
    access_token: Annotated[str | None, Depends(oauth2_schema)],
) -> User:
    return await auth_provider.get_current_user(
        access_token=access_token,
        request=request,
    )


def get_user(
    personal_token_policy: PersonalTokenPolicy = PersonalTokenPolicy.ALLOW,
) -> Callable[..., Coroutine[Any, Any, User]]:
    if personal_token_policy is PersonalTokenPolicy.REQUIRE:
        return get_user_via_personal_token
    if personal_token_policy is PersonalTokenPolicy.DENY:
        return get_user_via_access_token_or_cookie
    return get_user_via_any_credentials_source
