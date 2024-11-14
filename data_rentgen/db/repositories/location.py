# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Sequence

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Select,
    SQLColumnExpression,
    any_,
    asc,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import LocationDTO, PaginationDTO
from data_rentgen.exceptions.entity import EntityNotFoundError


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

    async def paginate(
        self,
        page: int,
        page_size: int,
        location_ids: Sequence[int],
        location_type: str | None,
        search_query: str | None,
    ) -> PaginationDTO[Location]:
        where = []

        if location_ids:
            where.append(Location.id == any_(location_ids))  # type: ignore[arg-type]

        if location_type:
            where.append(Location.type == location_type)

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            location_stmt = select(Location, ts_rank(Location.search_vector, tsquery).label("search_rank")).where(
                ts_match(Location.search_vector, tsquery),
                *where,
            )
            address_stmt = (
                select(Location, func.max(ts_rank(Address.search_vector, tsquery)).label("search_rank"))
                .join(Location, Address.location_id == Location.id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Location.id, Address.id)
            )

            union_cte = union(location_stmt, address_stmt).cte()

            location_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *location_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*location_columns)
            order_by = [desc("search_rank"), asc("name"), asc("type")]
        else:
            query = select(Location).where(*where)
            order_by = [Location.name, Location.type]

        options = [selectinload(Location.addresses)]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def update_external_id(self, location_id: int, external_id: str | None) -> Location:
        query = select(Location).where(Location.id == location_id).options(selectinload(Location.addresses))
        location = await self._session.scalar(query)
        if not location:
            raise EntityNotFoundError("Location", "id", location_id)
        location.external_id = external_id
        await self._session.flush([location])
        return location

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
        # in most cases, Location is unchanged, so we can avoid UPDATE statements
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
