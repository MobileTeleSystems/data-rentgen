# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.repositories.column_lineage import ColumnLineageRepository
from data_rentgen.db.repositories.dataset import DatasetRepository
from data_rentgen.db.repositories.dataset_column_relation import (
    DatasetColumnRelationRepository,
)
from data_rentgen.db.repositories.dataset_symlink import DatasetSymlinkRepository
from data_rentgen.db.repositories.input import InputRepository
from data_rentgen.db.repositories.job import JobRepository
from data_rentgen.db.repositories.job_type import JobTypeRepository
from data_rentgen.db.repositories.location import LocationRepository
from data_rentgen.db.repositories.operation import OperationRepository
from data_rentgen.db.repositories.output import OutputRepository
from data_rentgen.db.repositories.run import RunRepository
from data_rentgen.db.repositories.schema import SchemaRepository
from data_rentgen.db.repositories.user import UserRepository
from data_rentgen.dependencies import Stub


class UnitOfWork:
    def __init__(
        self,
        session: Annotated[AsyncSession, Depends(Stub(AsyncSession))],
    ):
        self._session = session
        self.location = LocationRepository(session)
        self.job_type = JobTypeRepository(session)
        self.job = JobRepository(session)
        self.run = RunRepository(session)
        self.operation = OperationRepository(session)
        self.dataset = DatasetRepository(session)
        self.dataset_symlink = DatasetSymlinkRepository(session)
        self.schema = SchemaRepository(session)
        self.input = InputRepository(session)
        self.output = OutputRepository(session)
        self.dataset_column_relation = DatasetColumnRelationRepository(session)
        self.column_lineage = ColumnLineageRepository(session)
        self.user = UserRepository(session)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self._session.rollback()
        else:
            await self._session.commit()
