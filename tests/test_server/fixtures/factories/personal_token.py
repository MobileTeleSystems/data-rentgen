from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING

from data_rentgen.db.models.personal_token import PersonalToken
from data_rentgen.utils.uuid import generate_new_uuid
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from data_rentgen.db.models import User


def personal_token_factory(**kwargs):
    since = kwargs.pop("since", None) or datetime.now(tz=UTC).date() - timedelta(days=5)
    token_id = generate_new_uuid(datetime.combine(since, datetime.min.time(), tzinfo=UTC))
    data = {
        "id": token_id,
        "name": random_string(16),
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
