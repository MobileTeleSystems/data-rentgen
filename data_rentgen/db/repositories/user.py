# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from data_rentgen.db.models import User
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import UserDTO


class UserRepository(Repository[User]):
    async def get_or_create(self, user: UserDTO) -> User:
        result = await self._get(user.name)
        if not result:
            await self._lock(user.name)
            result = await self._get(user.name) or await self._create(user)
        return result

    async def read_by_id(self, id_: int) -> User | None:
        statement = select(User).where(User.id == id_)
        return await self._session.scalar(statement)

    async def _get(self, name: str) -> User | None:
        statement = select(User).where(User.name == name)
        return await self._session.scalar(statement)

    async def _create(self, user: UserDTO) -> User:
        result = User(name=user.name)
        self._session.add(result)
        await self._session.flush([result])
        return result
