# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import LocationDTO


class LocationRepository(Repository[Location]):
    async def get_or_create(self, location: LocationDTO) -> Location:
        by_name = select(Location).where(Location.type == location.type, Location.name == location.name)
        by_addresses = (
            select(Location)
            .join(Location.addresses)
            .where(Location.type == location.type, Address.url.in_(location.addresses))
        )
        statement = (
            select(Location).from_statement(by_name.union(by_addresses)).options(selectinload(Location.addresses))
        )

        result = await self._session.scalar(statement)
        if not result:
            result = Location(type=location.type, name=location.name)
            self._session.add(result)
            await self._session.flush([result])

        existing_urls = {address.url for address in result.addresses}
        new_urls = set(location.addresses) - existing_urls
        if new_urls:
            addresses = [Address(url=url, location_id=result.id) for url in new_urls]
            result.addresses.extend(addresses)

        await self._session.flush([result])
        return result
