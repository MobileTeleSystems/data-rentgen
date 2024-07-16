# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from data_rentgen.db.models import User
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import UserDTO


class UserRepository(Repository[User]):
    async def get_or_create(self, user: UserDTO) -> User:
        statement = select(User).where(User.name == user.name)
        result = await self._session.scalar(statement)
        if not result:
            result = User(name=user.name)
            self._session.add(result)
            await self._session.flush([result])

        return result
