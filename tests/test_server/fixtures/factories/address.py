from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

from data_rentgen.db.models import Address
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def address_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "url": random_string(32),
    }
    data.update(kwargs)
    return Address(**data)


async def create_address(
    async_session: AsyncSession,
    location_id: int,
    address_kwargs: dict | None = None,
) -> Address:
    if address_kwargs:
        address_kwargs.update({"location_id": location_id})
    else:
        address_kwargs = {"location_id": location_id}
    address = address_factory(**address_kwargs)
    del address.id
    async_session.add(address)
    await async_session.commit()
    await async_session.refresh(address)
    return address
