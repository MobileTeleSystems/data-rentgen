#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import re
import sys
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Literal

from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)

PARTITION_GRANULARITY_PATERN = re.compile(r"(\w+)_y(\d{4})(?:_m(\d{1,2})(?:_d(\d{1,2}))?)?")


granularity_output_format = {
    "year": "y%Y",
    "month": "y%Y_m%m",
    "day": "y%Y_m%m_d%d",
}


class Command(str, Enum):
    DRY_RUN = "dry_run"
    DETATCH_PARTITIONS = "detach_partitions"
    REMOVE_DATA = "remove_data"
    TRUNCATE = "truncate"


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.clean_partitions command truncate --keep-after $(date -v2m '+%Y-%m-%d')",  # noqa: E501
        description="Truncate or detach partitions, before provided date.",
    )
    parser.add_argument(
        "command",
        choices=[item.value for item in Command],
        default=Command.DRY_RUN.value,
        nargs="?",
        help="Operation mode.",
    )
    parser.add_argument(
        "--keep-after",
        type=isoparse,
        default=(datetime.now(tz=UTC) - relativedelta(days=1)),
        nargs="?",
        help="Partitions with data before this date will be considered for cleanup",
    )
    return parser


@dataclass()
class Partition:
    partitions: list[date] = field(default_factory=list)
    granularity: Literal["year", "month", "day"] = field(default="year")


async def get_partitioned_tables(session: AsyncSession) -> dict[str, Partition] | None:
    tables: dict[str, Partition] = defaultdict(Partition)
    query = text(
        "select c.relname as table_name from pg_class c where c.relispartition = True and c.relkind = 'r' order by c.relname",  # noqa: E501
    )
    result = await session.execute(query)
    table_names = result.all()
    if not table_names:
        logger.info("There is no partitioned tables")
        return tables
    # parse tabel_names
    for (tabel_name,) in table_names:
        granularity: Literal["year", "month", "day"] = "year"
        match = re.search(PARTITION_GRANULARITY_PATERN, tabel_name)
        (name, year, month, day) = match.groups()  # type: ignore[union-attr]
        year = int(year)
        if month:
            month = int(month)
            granularity = "month"
        else:
            month = 1
        if day:
            day = int(day)
            granularity = "day"
        else:
            day = 1
        tables[name].partitions.append(date(year, month, day))
        tables[name].granularity = granularity
    return tables


def get_partitions(tables: dict[str, Partition], end_date: date):
    for table_name, value in tables.items():
        partitions_to_remove = [partition for partition in value.partitions if partition < end_date]
        tables[table_name].partitions = partitions_to_remove
    return tables


def get_query(tables: dict[str, Partition], query_type: str):
    queries = []
    query_template = ""
    match query_type:
        case "detach":
            query_template = "ALTER TABLE {table_name} DETACH PARTITION {table_name}_{partition};"
        case "remove":
            query_template = "DROP TABLE {table_name}_{partition};"
        case "truncate":
            query_template = "TRUNCATE TABLE {table_name}_{partition};"
    for table_name, value in tables.items():
        queries_per_table = [
            query_template.format(
                table_name=table_name,
                partition=partition.strftime(granularity_output_format[value.granularity]),
            )
            for partition in value.partitions
        ]
        queries.extend(queries_per_table)
    return queries


def show_removing_partitions(tables: dict[str, Partition]):
    for table_name, value in tables.items():
        logger.info("Removing next partitions for table: %s", table_name)
        partitions_names = " ".join(
            [
                f"{table_name}_{partition.strftime(granularity_output_format[value.granularity])}"
                for partition in value.partitions
            ],
        )
        logger.info(partitions_names)


async def detach_partitions(tables: dict[str, Partition], session: AsyncSession):
    partitions_to_detach = get_query(tables, "detach")
    for query in partitions_to_detach:
        logger.info("Detach partitions with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def remove_data(tables: dict[str, Partition], session: AsyncSession):
    partitions_to_remove = get_query(tables, "remove")
    for query in partitions_to_remove:
        logger.info("Remove table with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def truncate_data(tables: dict[str, Partition], session: AsyncSession):
    partitions_to_truncate = get_query(tables, "truncate")
    for query in partitions_to_truncate:
        logger.info("Truncate partition with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def main(args: list[str]) -> None:
    setup_logging(LoggingSettings())
    parser = get_parser()
    params = parser.parse_args(args)
    logger.info("Starting clean partition script with params: %s", params)
    end_date = params.keep_after.date()
    db_settings = DatabaseSettings()  # type: ignore[call-arg]
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        tables = await get_partitioned_tables(session)
        tables_to_remove = get_partitions(
            tables.copy(),  # type: ignore[union-attr]
            end_date,
        )
        match params.command:
            case "dry_run":
                show_removing_partitions(tables_to_remove)
            case "detach_partitions":
                logger.info("detach_partitions")
                await detach_partitions(tables_to_remove, session)
            case "remove_data":
                logger.info("remove_data")
                await detach_partitions(tables_to_remove, session)
                await remove_data(tables_to_remove, session)
            case "truncate":
                logger.info("truncate")
                await truncate_data(tables_to_remove, session)
            case _:
                logger.error("No such command: %s", params.command)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
