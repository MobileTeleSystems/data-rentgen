from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Job, Location, Run


async def enrich_runs(runs: list[Run], async_session: AsyncSession) -> list[Run]:
    query = select(Run).where(Run.id.in_([run.id for run in runs])).options(selectinload(Run.started_by_user))
    result = await async_session.scalars(query)
    return list(result.all())


async def enrich_datasets(datasets: list[Dataset], async_session: AsyncSession) -> list[Dataset]:
    query = (
        select(Dataset)
        .where(Dataset.id.in_([dataset.id for dataset in datasets]))
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    return list(result.all())


async def enrich_jobs(jobs: list[Job], async_session: AsyncSession) -> list[Job]:
    query = (
        select(Job)
        .where(Job.id.in_([job.id for job in jobs]))
        .options(selectinload(Job.location).selectinload(Location.addresses))
    )
    result = await async_session.scalars(query)
    return list(result.all())
