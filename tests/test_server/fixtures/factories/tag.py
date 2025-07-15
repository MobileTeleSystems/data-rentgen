from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

from data_rentgen.db.models import Tag
from data_rentgen.db.models.tag_value import TagValue
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def tag_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "name": random_string(32),
    }

    data.update(kwargs)
    return Tag(**data)


async def create_tag(
    async_session: AsyncSession,
    tag_kwargs: dict | None = None,
) -> Tag:
    tag_kwargs = tag_kwargs or {}
    tag = tag_factory(**tag_kwargs)
    del tag.id
    async_session.add(tag)
    await async_session.commit()
    await async_session.refresh(tag)
    return tag


def tag_value_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "value": random_string(32),
        "tag_id": randint(0, 10000000),
    }
    data.update(kwargs)
    return TagValue(**data)


async def create_tag_value(
    async_session: AsyncSession,
    tag_id: int,
    tag_value_kwargs: dict | None = None,
) -> TagValue:
    if tag_value_kwargs:
        tag_value_kwargs.update({"tag_id": tag_id})
    else:
        tag_value_kwargs = {"tag_id": tag_id}
    tag_value = tag_value_factory(**tag_value_kwargs)
    del tag_value.id
    async_session.add(tag_value)
    await async_session.commit()
    await async_session.refresh(tag_value)
    return tag_value
