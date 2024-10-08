# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import any_, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import LocationDTO


class LocationRepository(Repository[Location]):
    async def get_or_create(self, location: LocationDTO) -> Location:
        result = await self._get(location)
        if not result:
            await self._lock(location.type, location.name)
            result = await self._get(location) or await self._create(location)

        await self._update_addresses(result, location)
        return result

    async def _get(self, location: LocationDTO) -> Location | None:
        by_name = select(Location).where(Location.type == location.type, Location.name == location.name)
        by_addresses = (
            select(Location)
            .join(Location.addresses)
            .where(
                Location.type == location.type,
                Address.url == any_(location.addresses),  # type: ignore[arg-type]
            )
        )
        statement = (
            select(Location).from_statement(by_name.union(by_addresses)).options(selectinload(Location.addresses))
        )

        return await self._session.scalar(statement)

    async def _create(self, location: LocationDTO) -> Location:
        result = Location(type=location.type, name=location.name)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update_addresses(self, existing: Location, new: LocationDTO) -> Location:
        existing_urls = {address.url for address in existing.addresses}
        new_urls = set(new.addresses) - existing_urls
        if new_urls:
            addresses = [Address(url=url, location_id=existing.id) for url in new_urls]
            existing.addresses.extend(addresses)
            await self._session.flush([existing])
        return existing
