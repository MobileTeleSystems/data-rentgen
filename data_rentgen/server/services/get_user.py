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
        # dummy auth stores access_token in "Authorization" header
        user = await auth_provider.get_current_user(
            access_token=access_token,
            request=request,
        )
        if user is None:
            raise EntityNotFoundError("User not found")  # type: ignore[call-arg]
        return user

    return wrapper
