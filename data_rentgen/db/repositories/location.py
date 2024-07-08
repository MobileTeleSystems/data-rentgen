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
            .where(Location.type == location.type, Address.url.in_(location.urls))
        )
        statement = (
            select(Location).from_statement(by_name.union(by_addresses)).options(selectinload(Location.addresses))
        )

        result = await self._session.scalar(statement)
        changed: bool = False
        if not result:
            result = Location(type=location.type, name=location.name)
            self._session.add(result)
            changed = True

        existing_urls = {address.url for address in result.addresses}
        new_urls = set(location.urls) - existing_urls
        for url in new_urls:
            # currently we automatically add new addresses, but not delete old ones
            result.addresses.append(Address(url=url))
            changed = True

        if changed:
            await self._session.flush()
        return result
