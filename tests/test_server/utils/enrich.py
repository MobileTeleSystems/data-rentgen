from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Dataset, Job, Location, Run, TagValue


async def enrich_runs(runs: list[Run], async_session: AsyncSession) -> list[Run]:
    run_ids = [run.id for run in runs]
    query = select(Run).where(Run.id.in_(run_ids)).options(selectinload(Run.started_by_user))
    result = await async_session.scalars(query)
    runs_by_id = {run.id: run for run in result.all()}
    # preserve original order
    return [runs_by_id[run_id] for run_id in run_ids]


async def enrich_datasets(datasets: list[Dataset], async_session: AsyncSession) -> list[Dataset]:
    dataset_ids = [dataset.id for dataset in datasets]
    query = (
        select(Dataset)
        .where(Dataset.id.in_(dataset_ids))
        .options(selectinload(Dataset.location).selectinload(Location.addresses))
        .options(selectinload(Dataset.tags).selectinload(TagValue.tag))
    )
    result = await async_session.scalars(query)
    datasets_by_id = {dataset.id: dataset for dataset in result.all()}
    # preserve original order
    return [datasets_by_id[dataset_id] for dataset_id in dataset_ids]


async def enrich_jobs(jobs: list[Job], async_session: AsyncSession) -> list[Job]:
    job_ids = [job.id for job in jobs]
    query = select(Job).where(Job.id.in_(job_ids)).options(selectinload(Job.location).selectinload(Location.addresses))
    result = await async_session.scalars(query)
    jobs_by_id = {job.id: job for job in result.all()}
    # preserve original order
    return [jobs_by_id[job_id] for job_id in job_ids]


async def enrich_locations(locations: list[Location], async_session: AsyncSession) -> list[Location]:
    location_ids = [location.id for location in locations]
    query = select(Location).where(Location.id.in_(location_ids)).options(selectinload(Location.addresses))
    result = await async_session.scalars(query)
    locations_by_id = {location.id: location for location in result.all()}
    # preserve original order
    return [locations_by_id[location_id] for location_id in location_ids]
