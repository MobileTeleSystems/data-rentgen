# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from time import time
from typing import Annotated, Any

from devtools import pformat
from fastapi import Depends, FastAPI

from data_rentgen.db.models import User
from data_rentgen.dependencies import Stub
from data_rentgen.dto import UserDTO
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.server.providers.auth.base_provider import AuthProvider
from data_rentgen.server.settings.auth.dummy import DummyAuthProviderSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt
from data_rentgen.services import UnitOfWork

logger = logging.getLogger(__name__)


class DummyAuthProvider(AuthProvider):
    def __init__(
        self,
        settings: Annotated[DummyAuthProviderSettings, Depends(Stub(DummyAuthProviderSettings))],
        unit_of_work: Annotated[UnitOfWork, Depends()],
    ) -> None:
        self._settings = settings
        self._uow = unit_of_work

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = DummyAuthProviderSettings.model_validate(app.state.settings.auth.dict(exclude={"provider"}))
        logger.info("Using %s provider with settings:\n%s", cls.__name__, pformat(settings))
        app.dependency_overrides[AuthProvider] = cls
        app.dependency_overrides[DummyAuthProviderSettings] = lambda: settings
        return app

    async def get_current_user(self, access_token: str, *args, **kwargs) -> User | None:
        if not access_token:
            raise AuthorizationError("Missing auth credentials")

        user_id = self._get_user_id_from_token(access_token)
        return await self._uow.user.read_by_id(user_id)

    async def get_token_password_grant(
        self,
        grant_type: str | None = None,
        login: str | None = None,
        password: str | None = None,
        scopes: list[str] | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> dict[str, Any]:
        if not login:
            raise AuthorizationError("Missing auth credentials")

        logger.info("Get/create user %r in database", login)
        async with self._uow:
            user = await self._uow.user._get(login)  # noqa: WPS437
            if not user:
                user = await self._uow.user._create(  # noqa: WPS437
                    UserDTO(name=login),
                )

        logger.info("User with id %r found", user.id)
        logger.info("Generate access token for user id %r", user.id)
        access_token, expires_at = self._generate_access_token(user_id=user.id)
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_at": expires_at,
        }

    async def get_token_authorization_code_grant(
        self,
        code: str,
        redirect_uri: str,
        scopes: list[str] | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError("Authorization code grant is not supported by DummyAuthProvider.")

    def _generate_access_token(self, user_id: int) -> tuple[str, float]:
        expires_at = time() + self._settings.access_token.expire_seconds
        payload = {
            "user_id": user_id,
            "exp": expires_at,
        }
        access_token = sign_jwt(
            payload,
            self._settings.access_token.secret_key.get_secret_value(),
            self._settings.access_token.security_algorithm,
        )
        return access_token, expires_at

    def _get_user_id_from_token(self, token: str) -> int:
        try:
            payload = decode_jwt(
                token,
                self._settings.access_token.secret_key.get_secret_value(),
                self._settings.access_token.security_algorithm,
            )
            return int(payload["user_id"])
        except (KeyError, TypeError, ValueError) as e:
            raise AuthorizationError("Invalid token") from e
