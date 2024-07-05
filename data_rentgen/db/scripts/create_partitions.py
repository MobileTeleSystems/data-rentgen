#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import sys
from argparse import ArgumentParser
from datetime import date, datetime
from enum import Enum

from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.models.interaction import Interaction
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)

PARTITIONED_TABLES = [Run.__tablename__, Operation.__tablename__, Interaction.__tablename__]


class Granularity(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    def __str__(self) -> str:
        return self.value

    def round(self, input: date) -> date:
        if self == Granularity.DAY:
            return input
        if self == Granularity.MONTH:
            return input.replace(day=1)
        return input.replace(day=1, month=1)

    def to_range(self) -> relativedelta:
        if self == Granularity.DAY:
            return relativedelta(days=1)
        if self == Granularity.MONTH:
            return relativedelta(months=1)
        return relativedelta(years=1)

    def to_format(self) -> str:
        if self == Granularity.DAY:
            return "y%Y_m%m_%d"
        if self == Granularity.MONTH:
            return "y%Y_m%m"
        return "y%Y"


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.create_partitions --start 2024-01 --granularity month",
        description="Create partitions for tables, for given start date and granularity.",
    )
    parser.add_argument(
        "--start",
        type=isoparse,
        default=datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        nargs="?",
        help="Start date for partitions, default is the first day of current month.",
    )
    parser.add_argument(
        "--end",
        type=isoparse,
        nargs="?",
        help="End date for partitions, default is the first day of next month.",
    )
    parser.add_argument(
        "--granularity",
        type=lambda x: Granularity[x].value,
        choices=[item.value for item in Granularity],
        default=Granularity.MONTH,
        nargs="?",
        help="Granularity of partitions, default is month.",
    )
    return parser


def generate_partition_range(start: date, end: date, granularity: Granularity) -> list[tuple[date, date]]:
    parts = []
    current = start
    next = current + granularity.to_range()
    while current < end:
        parts.append((current, next))
        current += granularity.to_range()
        next = current + granularity.to_range()
    if not parts:
        raise ValueError("Data range is too small")

    return parts


def generate_partition_statements(start: date, end: date, granularity: Granularity) -> list[str]:
    name = start.strftime(granularity.to_format())

    start_str = start.isoformat()
    end_str = end.isoformat()

    return [
        f"CREATE TABLE IF NOT EXISTS {Run.__tablename__}_{name} PARTITION OF {Run.__tablename__} FOR VALUES FROM ('{start_str}') TO ('{end_str}')",
        f"CREATE TABLE IF NOT EXISTS {Operation.__tablename__}_{name} PARTITION OF {Operation.__tablename__} FOR VALUES FROM ('{start_str}') TO ('{end_str}')",
        f"CREATE TABLE IF NOT EXISTS {Interaction.__tablename__}_{name} PARTITION OF {Interaction.__tablename__} FOR VALUES FROM ('{start_str}') TO ('{end_str}')",
    ]


async def create_partition(start: date, end: date, granularity: Granularity, session: AsyncSession):
    partition_name = start.strftime(granularity.to_format())

    start_str = start.isoformat()
    end_str = end.isoformat()

    for table in PARTITIONED_TABLES:
        statement = f"CREATE TABLE IF NOT EXISTS {table}_{partition_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')"
        logger.debug("Executing statement: %s", statement)
        await session.execute(text(statement))


async def main(args: list[str]) -> None:
    setup_logging(LoggingSettings())

    parser = get_parser()
    params = parser.parse_args(args)
    if params.end is None:
        params.end = params.start + params.granularity.to_range()

    granularity = Granularity(params.granularity)
    start, end = granularity.round(params.start.date()), granularity.round(params.end.date())
    if start > end:
        raise ValueError("Start date must be less than end date.")

    logger.info("Creating partitions from %s to %s with granularity %s", start, end, granularity.value)

    db_settings = DatabaseSettings()
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        for from_, to in generate_partition_range(start, end, granularity):
            await create_partition(from_, to, granularity, session)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
