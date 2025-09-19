# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import any_, func, select

from data_rentgen.db.models import User
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import UserDTO


class UserRepository(Repository[User]):
    async def fetch_bulk(self, users_dto: list[UserDTO]) -> list[tuple[UserDTO, User | None]]:
        if not users_dto:
            return []
        unique_keys = [user_dto.name.lower() for user_dto in users_dto]
        statement = select(User).where(
            func.lower(User.name) == any_(unique_keys),  # type: ignore[arg-type]
        )
        scalars = await self._session.scalars(statement)
        existing = {user.name.lower(): user for user in scalars.all()}
        return [(user_dto, existing.get(user_dto.name.lower())) for user_dto in users_dto]

    async def create(self, user_dto: UserDTO) -> User:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(user_dto.name)
        return await self.get_or_create(user_dto)

    async def get_or_create(self, user_dto: UserDTO) -> User:
        return await self._get(user_dto.name) or await self._create(user_dto)

    async def _get(self, name: str) -> User | None:
        statement = select(User).where(func.lower(User.name) == name.lower())
        return await self._session.scalar(statement)

    async def _create(self, user: UserDTO) -> User:
        result = User(name=user.name)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def read_by_id(self, id_: int) -> User | None:
        statement = select(User).where(User.id == id_)
        return await self._session.scalar(statement)
