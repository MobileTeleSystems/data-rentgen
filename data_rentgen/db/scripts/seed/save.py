# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from faststream import Logger
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.services.uow import UnitOfWork


async def save_to_db(
    data: BatchExtractionResult,
    session: AsyncSession,
    logger: Logger,
) -> None:
    """Save data to database.

    This is different from consumer's method because here we generate random data (unique),
    and there are no writers we conflict with. So we can use one transaction + bulk insert for runs.
    """
    async with UnitOfWork(session) as unit_of_work:
        logger.debug("Creating locations")
        for location_dto in data.locations():
            location = await unit_of_work.location.create_or_update(location_dto)
            location_dto.id = location.id

        logger.debug("Creating datasets")
        for dataset_dto in data.datasets():
            dataset = await unit_of_work.dataset.get_or_create(dataset_dto)
            dataset_dto.id = dataset.id

        logger.debug("Creating symlinks")
        for dataset_symlink_dto in data.dataset_symlinks():
            dataset_symlink = await unit_of_work.dataset_symlink.get_or_create(dataset_symlink_dto)
            dataset_symlink_dto.id = dataset_symlink.id

        logger.debug("Creating job types")
        for job_type_dto in data.job_types():
            job_type = await unit_of_work.job_type.get_or_create(job_type_dto)
            job_type_dto.id = job_type.id

        logger.debug("Creating jobs")
        for job_dto in data.jobs():
            job = await unit_of_work.job.create_or_update(job_dto)
            job_dto.id = job.id

        logger.debug("Creating sql queries")
        for sql_query_dto in data.sql_queries():
            sql_query = await unit_of_work.sql_query.get_or_create(sql_query_dto)
            sql_query_dto.id = sql_query.id

        logger.debug("Creating users")
        for user_dto in data.users():
            user = await unit_of_work.user.get_or_create(user_dto)
            user_dto.id = user.id

        logger.debug("Creating schemas")
        for schema_dto in data.schemas():
            schema = await unit_of_work.schema.get_or_create(schema_dto)
            schema_dto.id = schema.id

        logger.debug("Creating runs")
        await unit_of_work.run.create_or_update_bulk(data.runs())

        logger.debug("Creating operations")
        await unit_of_work.operation.create_or_update_bulk(data.operations())

        logger.debug("Creating inputs")
        await unit_of_work.input.create_or_update_bulk(data.inputs())

        logger.debug("Creating outputs")
        await unit_of_work.output.create_or_update_bulk(data.outputs())

        column_lineage = data.column_lineage()
        logger.debug("Creating dataset column relations")
        await unit_of_work.dataset_column_relation.create_bulk_for_column_lineage(column_lineage)

        logger.debug("Creating column lineage")
        await unit_of_work.column_lineage.create_bulk(column_lineage)
