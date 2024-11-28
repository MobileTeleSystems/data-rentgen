# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from fastapi import Depends, Request
from fastapi.security import OAuth2PasswordBearer

from data_rentgen.db.models import User
from data_rentgen.dependencies import Stub
from data_rentgen.exceptions import EntityNotFoundError
from data_rentgen.server.providers.auth import AuthProvider

oauth_schema = OAuth2PasswordBearer(tokenUrl="v1/auth/token", auto_error=False)


def get_user() -> Callable[[Request, AuthProvider, str], Coroutine[Any, Any, User]]:
    async def wrapper(
        request: Request,
        auth_provider: Annotated[AuthProvider, Depends(Stub(AuthProvider))],
        access_token: Annotated[str | None, Depends(oauth_schema)],
    ) -> User:
        # keycloak provider patches session and store access_token in cookie,
        # dummy auth stores access_token in "Authorization" header
        access_token = request.session.get("access_token", "") or access_token
        user = await auth_provider.get_current_user(
            access_token=access_token,
            request=request,
        )
        if user is None:
            raise EntityNotFoundError("User", "username", "None")  # type: ignore[call-arg]
        return user

    return wrapper
