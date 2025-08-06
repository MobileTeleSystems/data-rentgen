from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from sqlalchemy import delete

from data_rentgen.db.models.personal_token import PersonalToken
from data_rentgen.server.settings.auth.personal_token import PersonalTokenSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt
from data_rentgen.utils.uuid import generate_new_uuid
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy.ext.asyncio import AsyncSession

    from data_rentgen.db.models import User
    from data_rentgen.server.settings import ServerApplicationSettings


def personal_token_factory(**kwargs):
    since = kwargs.pop("since", None) or datetime.now(tz=UTC).date() - timedelta(days=5)
    token_id = generate_new_uuid(datetime.combine(since, datetime.min.time(), tzinfo=UTC))
    data = {
        "id": token_id,
        "name": "personal-token-" + random_string(16),
        "user_id": randint(0, 10000000),
        "scopes": ["all:read", "all:write"],
        "since": since,
        "until": datetime.now(tz=UTC).date() + timedelta(days=1),
        "revoked_at": None,
    }
    data.update(kwargs)
    return PersonalToken(**data)


async def create_personal_token(
    async_session: AsyncSession,
    user: User,
    token_kwargs: dict | None = None,
) -> PersonalToken:
    token_kwargs = token_kwargs or {}
    token = personal_token_factory(user_id=user.id, **token_kwargs)
    async_session.add(token)
    await async_session.commit()
    await async_session.refresh(token)

    return token


@pytest_asyncio.fixture(params=[{}])
async def personal_token(
    user: User,
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[PersonalToken, None]:
    params = request.param
    async with async_session_maker() as async_session:
        item = await create_personal_token(async_session, user, params)

        await async_session.commit()
        await async_session.refresh(item)

        async_session.expunge_all()

    yield item

    delete_query = delete(PersonalToken).where(PersonalToken.id == item.id)
    async with async_session_maker() as async_session:
        await async_session.execute(delete_query)
        await async_session.commit()


@pytest.fixture
def personal_token_settings(server_app_settings: ServerApplicationSettings) -> PersonalTokenSettings:
    return PersonalTokenSettings.model_validate(server_app_settings.auth.personal_tokens)


@pytest.fixture
def personal_token_jwt_generator(
    personal_token_settings: PersonalTokenSettings,
) -> Callable[[PersonalToken, User], str]:
    def _generate_personal_token(personal_token: PersonalToken, user: User, time_delta: int = 1000) -> str:
        now = time.time()
        jwt = sign_jwt(
            {
                "jti": str(personal_token.id),
                "iss": "data-rentgen",
                "token_name": personal_token.name,
                "sub_id": user.id,
                "preferred_username": user.name,
                "scope": " ".join(personal_token.scopes),
                "iat": 0,
                "nbf": 0,
                "exp": now + time_delta,
            },
            personal_token_settings.secret_key.get_secret_value(),  # type: ignore[union-attr]
            personal_token_settings.security_algorithm,
        )
        return f"personal_token_{jwt}"

    return _generate_personal_token


@pytest.fixture
def personal_token_jwt_decoder(personal_token_settings: PersonalTokenSettings) -> Callable[[str], dict]:
    def _decoder(jwt_string: str) -> dict:
        return decode_jwt(
            token=jwt_string.replace("personal_token_", ""),
            secret_key=personal_token_settings.secret_key.get_secret_value(),
            security_algorithm=personal_token_settings.security_algorithm,
        )

    return _decoder
