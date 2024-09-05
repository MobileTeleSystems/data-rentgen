from typing import Sequence

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Run


async def get_runs_from_db(runs: list[Run], async_session: AsyncSession) -> Sequence[Run]:
    query = select(Run).where(Run.id.in_([run.id for run in runs])).options(selectinload(Run.started_by_user))
    result = await async_session.scalars(query)
    return result.all()
