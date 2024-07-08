# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from faststream import Depends
from faststream.kafka import KafkaRouter
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import extract_job, extract_job_location
from data_rentgen.consumer.openlineage.job import OpenLineageJob
from data_rentgen.consumer.openlineage.job_facets.job_type import OpenLineageJobType
from data_rentgen.consumer.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.consumer.openlineage.run_facets.parent_run import OpenLineageParentJob
from data_rentgen.db.models.job import Job
from data_rentgen.dependencies import Stub
from data_rentgen.services.uow import UnitOfWork

router = KafkaRouter()


def get_unit_of_work(session: AsyncSession = Depends(Stub(AsyncSession))) -> UnitOfWork:
    return UnitOfWork(session)


@router.subscriber("input.runs")
async def runs_handler(event: OpenLineageRunEvent, unit_of_work: UnitOfWork = Depends(get_unit_of_work)):
    job_type_facet = event.job.facets.get("jobType", None)
    if job_type_facet and job_type_facet.jobType in {OpenLineageJobType.DAG, OpenLineageJobType.JOB}:
        # completely ignore Airflow DAGs.
        # temporary ignore Spark jobs
        return

    await lookup_job(event.job, unit_of_work)


async def lookup_job(job: OpenLineageJob | OpenLineageParentJob, unit_of_work: UnitOfWork) -> Job:
    job_raw = extract_job(job)
    job_location_raw = extract_job_location(job)

    async with unit_of_work:
        job_location = await unit_of_work.location.get_or_create(job_location_raw)
        return await unit_of_work.job.get_or_create(job_raw, job_location)
