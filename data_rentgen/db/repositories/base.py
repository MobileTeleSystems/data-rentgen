# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC
from typing import Generic, Tuple, TypeVar

from sqlalchemy import ScalarResult, Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import ColumnElement, SQLColumnExpression

from data_rentgen.db.models import Base
from data_rentgen.dto import PaginationDTO

Model = TypeVar("Model", bound=Base)


class Repository(ABC, Generic[Model]):
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self._session = session

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}()"

    @classmethod
    def model_type(cls) -> type[Model]:
        # Get `User` from `UserRepository(Repository[User])`
        return cls.__orig_bases__[0].__args__[0]  # type: ignore[attr-defined]

    async def _get(
        self,
        *where: ColumnElement,
    ) -> Model | None:
        model_type = self.model_type()
        query: Select = select(model_type).where(*where)
        return await self._session.scalar(query)

    async def _paginate(
        self,
        order_by: list[SQLColumnExpression],
        page: int,
        page_size: int,
        where: list[SQLColumnExpression] | None = None,
    ) -> PaginationDTO[Model]:
        model_type = self.model_type()
        query: Select = select(model_type)
        if where:
            query = query.where(*where)

        items_result: ScalarResult[Model] = await self._session.scalars(
            query.order_by(*order_by).limit(page_size).offset((page - 1) * page_size),
        )
        total_count: int = await self._session.scalar(  # type: ignore[assignment]
            select(func.count()).select_from(query.subquery()),
        )
        return PaginationDTO[model_type](  # type: ignore[valid-type]
            items=list(items_result.all()),
            total_count=total_count,
            page=page,
            page_size=page_size,
        )

    async def _paginate_by_query(
        self,
        order_by: list[SQLColumnExpression],
        page: int,
        page_size: int,
        query: Select[Tuple[Model]],
    ) -> PaginationDTO[Model]:
        model_type = self.model_type()
        items_result: ScalarResult[Model] = await self._session.scalars(
            query.order_by(*order_by).limit(page_size).offset((page - 1) * page_size),
        )
        total_count: int = await self._session.scalar(  # type: ignore[assignment]
            select(func.count()).select_from(query.subquery()),
        )
        return PaginationDTO[model_type](  # type: ignore[valid-type]
            items=list(items_result.all()),
            total_count=total_count,
            page=page,
            page_size=page_size,
        )

    async def _count(
        self,
        where: list[SQLColumnExpression] | None = None,
    ) -> int:
        model_type = self.model_type()
        query: Select = select(func.count()).select_from(model_type)
        if where:
            query = query.where(*where)

        result = await self._session.scalars(query)
        return result.one()
