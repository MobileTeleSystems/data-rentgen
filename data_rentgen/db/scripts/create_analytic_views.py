#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import sys
from argparse import ArgumentParser
from enum import Enum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


class Depths(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    def __str__(self) -> str:
        return self.value


view_sufix_map = {
    "day": "_daily_stats",
    "week": "_weekly_stats",
    "month": "_monthly_stats",
}


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.create_analytics_view --depths day",
        description="Create matherialized views based on input and output table with given depths",
    )
    parser.add_argument(
        "--depths",
        type=lambda x: Depths(x).value,
        choices=[item.value for item in Depths],
        nargs="?",
        help="Depth of matherialized view data (created_at filter). Default is day",
    )
    parser.add_argument(
        "-r",
        "--refresh",
        action="store_true",
        default=False,
        help="If provide will update views",
    )
    return parser


def get_statement(base_table: str, type: str) -> str:
    view_name = base_table + view_sufix_map[type]
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
        AS (
            WITH aggregates AS (
            SELECT
                {base_table}.dataset_id as dataset_id
                , u.id as user_id
                , u.name as user_name
                , max({base_table}.created_at) as last_interaction
                , count(*) as num_of_interactions
                , sum(num_bytes) as s_bytes
                , sum(num_rows) as s_rows
                , sum(num_files) as s_files
                FROM {base_table}
                JOIN run r ON {base_table}.run_id = r.id
                JOIN public.user u ON r.started_by_user_id = u.id
                WHERE {base_table}.created_at >= now() - interval '1 {type}'
                GROUP BY {base_table}.dataset_id, u.id
            )
            SELECT
                d.name as dataset_name
                , l.name as location_name
                , agr.user_id
                , agr.user_name
                , agr.last_interaction
                , agr.num_of_interactions
                , agr.s_bytes
                , agr.s_rows
                , agr.s_files
            FROM aggregates agr
            JOIN dataset d ON agr.dataset_id = d.id
            LEFT JOIN location l ON d.location_id = l.id
        )
        WITH DATA
    """


async def create_views(depths: Depths, session: AsyncSession):
    for base_table in ("output", "input"):
        statement = get_statement(base_table, depths)
        logger.debug("Executing statement: %s", statement)
        await session.execute(text(statement))


async def refresh_view(view_name: str, session: AsyncSession):
    statement = f"REFRESH MATERIALIZED VIEW {view_name}"
    logger.debug("Executing statement: %s", statement)
    await session.execute(text(statement))


async def main(args: list[str]) -> None:
    setup_logging(LoggingSettings())

    parser = get_parser()
    params = parser.parse_args(args)
    depths = params.depths
    refresh = params.refresh

    db_settings = DatabaseSettings()
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        if depths:
            depths = Depths(depths)
            logger.info("Create views with depths: %s", depths)
            await create_views(depths, session)
            await session.commit()
        if refresh:
            logger.info("Refresh views")
            for suffix in view_sufix_map.values():
                for base_name in ("output", "input"):
                    await refresh_view(base_name + suffix, session)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
