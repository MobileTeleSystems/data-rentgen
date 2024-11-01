# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import any_, select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import LocationDTO


class LocationRepository(Repository[Location]):
    async def create_or_update(self, location: LocationDTO) -> Location:
        result = await self._get(location)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(location.type, location.name)
            result = await self._get(location)

        if not result:
            return await self._create(location)

        await self._update_addresses(result, location)
        return result

    async def _get(self, location: LocationDTO) -> Location | None:
        by_name = select(Location).where(Location.type == location.type, Location.name == location.name)
        by_addresses = (
            select(Location)
            .join(Location.addresses)
            .where(
                Location.type == location.type,
                Address.url == any_(sorted(location.addresses)),  # type: ignore[arg-type]
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
        new_urls = new.addresses - existing_urls
        if not new_urls:
            return existing

        # take a lock, to avoid race conditions, and then
        # get fresh state of the object, because it already could be updated by another worker
        await self._lock(existing.type, existing.name)
        await self._session.refresh(existing, ["addresses"])

        # already has all required addresses - nothing to update
        existing_urls = {address.url for address in existing.addresses}
        new_urls = new.addresses - existing_urls
        if not new_urls:
            return existing

        # add new addresses while holding the lock
        addresses = [Address(url=url, location_id=existing.id) for url in new_urls]
        existing.addresses.extend(addresses)
        await self._session.flush([existing])
        return existing
