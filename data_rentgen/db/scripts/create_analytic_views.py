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


class Depth(str, Enum):
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
        type=Depth,
        choices=[item.value for item in Depth],
        nargs="+",
        help="Depth of matherialized view data (created_at filter). You can provide list of args",
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
                , max({base_table}.created_at) as last_interaction_dt
                , count(*) as num_of_interactions
                , sum(num_bytes) as sum_bytes
                , sum(num_rows) as sum_rows
                , sum(num_files) as sum_files
                FROM {base_table}
                JOIN run r ON {base_table}.run_id = r.id
                JOIN public.user u ON r.started_by_user_id = u.id
                WHERE {base_table}.created_at >= now() - interval '1 {type}'
                GROUP BY {base_table}.dataset_id, u.id
            )
            SELECT
                d.name as dataset_name
                , l.name as dataset_location
                , l.type as dataset_location_type
                , agr.user_id
                , agr.user_name
                , agr.last_interaction_dt
                , agr.num_of_interactions
                , agr.sum_bytes
                , agr.sum_rows
                , agr.sum_files
            FROM aggregates agr
            JOIN dataset d ON agr.dataset_id = d.id
            LEFT JOIN location l ON d.location_id = l.id
        )
    """


async def create_view(depth: Depth, session: AsyncSession):
    for base_table in ("output", "input"):
        statement = get_statement(base_table, depth)
        logger.debug("Executing statement: %s", statement)
        await session.execute(text(statement))


async def refresh_view(depth: Depth, session: AsyncSession):
    for base_table in ("output", "input"):
        view_name = base_table + view_sufix_map[depth]
        logger.info("Refresh view: %s", view_name)
        statement = f"REFRESH MATERIALIZED VIEW {view_name}"
        await session.execute(text(statement))


async def main(args: list[str]) -> None:
    setup_logging(LoggingSettings())

    parser = get_parser()
    params = parser.parse_args(args)
    depths = params.depths
    if not depths:
        logger.info("Create views for all depths")
        depths = Depth
    else:
        depths = sorted(set(depths))

    db_settings = DatabaseSettings()
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        for depth in depths:
            await create_view(depth, session)
            await refresh_view(depth, session)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
