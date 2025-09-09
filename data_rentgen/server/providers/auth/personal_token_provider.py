# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from datetime import UTC, datetime, timedelta
from pprint import pformat
from typing import Annotated, Any, Literal
from uuid import UUID

from cachetools import LRUCache, TTLCache
from fastapi import Depends, FastAPI, Request

from data_rentgen.db.factory import AsyncSession
from data_rentgen.db.models import PersonalToken, User
from data_rentgen.dependencies import Stub
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.exceptions.entity import EntityNotFoundError
from data_rentgen.server.services.personal_token import PersonalTokenService
from data_rentgen.server.settings.auth.personal_token import PersonalTokenSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils.uuid import extract_timestamp_from_uuid

logger = logging.getLogger(__name__)


class PersonalTokenCache:
    def __init__(self, maxsize: int, ttl_seconds: int):
        self.alive_cache: TTLCache[UUID, Literal[True]] = TTLCache(
            maxsize=maxsize,
            ttl=ttl_seconds,
        )
        # revoked tokens cannot be resurrected, no TTL
        self.revoked_cache: LRUCache[UUID, Literal[True]] = LRUCache(maxsize=maxsize)

    def is_revoked(self, token_id: UUID) -> bool | None:
        if token_id in self.revoked_cache:
            return True
        if token_id in self.alive_cache:
            return False
        return None

    def add_token(self, token_id: UUID) -> None:
        self.alive_cache[token_id] = True

    def revoke_token(self, token_id: UUID) -> None:
        self.revoked_cache[token_id] = True
        self.alive_cache.pop(token_id, None)


class PersonalTokenAuthProvider:
    """Auth provider which only accepts personal tokens.

    Used internally by DataRentgen. Cannot not be used as a regular AuthProvider.
    """

    def __init__(
        self,
        settings: Annotated[PersonalTokenSettings, Depends(Stub(PersonalTokenSettings))],
        token_cache: Annotated[PersonalTokenCache, Depends(Stub(PersonalTokenCache))],
    ) -> None:
        self._settings = settings
        self._token_cache = token_cache

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = PersonalTokenSettings.model_validate(
            app.state.settings.auth.personal_tokens,
        )
        token_cache = PersonalTokenCache(
            maxsize=settings.cache_size,
            ttl_seconds=settings.cache_ttl_seconds,
        )
        logger.info("Initializing %s provider with settings:\n%s", cls.__name__, pformat(settings))

        async def get_settings():
            return settings

        async def get_token_cache():
            return token_cache

        app.dependency_overrides[PersonalTokenSettings] = get_settings
        app.dependency_overrides[PersonalTokenCache] = get_token_cache
        return app

    def generate_jwt(
        self,
        user: User,
        token: PersonalToken,
    ) -> str:
        since = datetime.combine(token.since, datetime.min.time(), tzinfo=UTC)
        until = datetime.combine(token.until, datetime.min.time(), tzinfo=UTC) + timedelta(days=1)
        claims = {
            "jti": str(token.id),
            "token_name": token.name,
            "sub_id": user.id,
            "preferred_username": user.name,
            "scope": " ".join(token.scopes),
            "iat": int(extract_timestamp_from_uuid(token.id).timestamp()),
            "nbf": since.timestamp(),
            "exp": until.timestamp(),
        }
        jwt = sign_jwt(
            payload=claims,
            secret_key=self._settings.secret_key.get_secret_value(),  # type: ignore[union-attr]
            security_algorithm=self._settings.security_algorithm,
        )
        return f"personal_token_{jwt}"

    def extract_token(
        self,
        jwt_string: str,
    ) -> tuple[User, PersonalToken]:
        claims = decode_jwt(
            token=jwt_string.replace("personal_token_", ""),
            secret_key=self._settings.secret_key.get_secret_value(),  # type: ignore[union-attr]
            security_algorithm=self._settings.security_algorithm,
        )
        try:
            token = PersonalToken(
                id=UUID(claims["jti"]),
                since=datetime.fromtimestamp(float(claims["nbf"]), tz=UTC),
                until=datetime.fromtimestamp(float(claims["exp"]), tz=UTC),
                name=claims["token_name"],
                user_id=claims["sub_id"],
                scopes=claims["scope"].split(" "),
            )
            user = User(id=int(claims["sub_id"]), name=claims["preferred_username"])
        except (KeyError, TypeError, ValueError) as e:
            msg = "Invalid token"
            raise AuthorizationError(msg) from e
        return user, token

    async def get_current_user(self, access_token: str | None, request: Request) -> User:
        if not access_token:
            msg = "Missing Authorization header"
            raise AuthorizationError(msg)

        if not access_token.startswith("personal_token_"):
            details = "Access token was passed but PersonalToken was expected"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)

        if not self._settings.enabled:
            msg = "Authentication using PersonalTokens is disabled"
            raise AuthorizationError(msg)

        user, token = self.extract_token(access_token)
        is_revoked = await self.check_token_revoked(user, token, request)
        if is_revoked:
            details = f"PersonalToken name='{token.name}', id={token.id} is revoked"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)
        logger.debug("Got user %r from token %r", user, token)
        return user

    async def get_optional_user(self, access_token: str | None, request: Request) -> User | None:
        if not access_token or not access_token.startswith("personal_token_"):
            return None

        if not self._settings.enabled:
            msg = "Authentication using PersonalTokens is disabled"
            raise AuthorizationError(msg)

        user, token = self.extract_token(access_token)
        is_revoked = await self.check_token_revoked(user, token, request)
        if is_revoked:
            details = f"PersonalToken name='{token.name}', id={token.id} is revoked"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)
        logger.debug("Got user %r from token %r", user, token)
        return user

    async def check_token_revoked(self, user: User, token: PersonalToken, request: Request) -> bool:
        is_revoked = self._token_cache.is_revoked(token.id)
        if is_revoked is not None:
            return is_revoked

        is_revoked = False

        # checking session in cache is fast, creating new session is slow,
        # let's postpone it using a hack
        session_generator = request.app.dependency_overrides[AsyncSession]
        async for session in session_generator():
            personal_token_service = PersonalTokenService(
                uow=UnitOfWork(session),
                settings=self._settings,
            )
            try:
                await personal_token_service.get(user, token.id)
                is_revoked = False
            except EntityNotFoundError:
                is_revoked = True

        if is_revoked:
            self._token_cache.revoke_token(token.id)
        else:
            self._token_cache.add_token(token.id)
        return is_revoked

    async def get_token_password_grant(
        self,
        login: str,
        password: str,
    ) -> dict[str, Any]:
        msg = "Password grant is not supported by PersonalTokenAuthProvider"
        raise NotImplementedError(msg)

    async def get_token_authorization_code_grant(
        self,
        code: str,
    ) -> dict[str, Any]:
        msg = "Authorization code grant is not supported by PersonalTokenAuthProvider"
        raise NotImplementedError(msg)

    async def logout(self, user: User, refresh_token: str | None) -> None:
        msg = "Logout method is not implemented for PersonalTokenAuthProvider"
        raise NotImplementedError(msg)
