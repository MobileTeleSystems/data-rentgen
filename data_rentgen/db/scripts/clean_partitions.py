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
from data_rentgen.db.models import Input, Operation, Output, Run
from data_rentgen.db.models.column_lineage import ColumnLineage
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)

PARTITION_GRANULARITY_PATERN = re.compile(r"(\w+)_y(\d{4})(?:_m(\d{1,2})(?:_d(\d{1,2}))?)?")

PARTITIONED_TABLES = (
    Run.__tablename__,
    Operation.__tablename__,
    Input.__tablename__,
    Output.__tablename__,
    ColumnLineage.__tablename__,
)

granularity_output_format = {
    "year": "_y%Y",
    "month": "_y%Y_m%m",
    "day": "_y%Y_m%m_d%d",
}


class Command(str, Enum):
    DRY_RUN = "dry_run"
    DETATCH_PARTITIONS = "detach_partitions"
    REMOVE_DATA = "remove_data"
    TRUNCATE = "truncate"


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.clean_partitions truncate --keep-after 2025-01-01",
        description="Truncate or detach partitions, before provided date.",
    )
    parser.add_argument(
        "command",
        type=Command,
        choices=[item.value for item in Command],
        default=Command.DRY_RUN.value,
        nargs="?",
        help="Operation mode",
    )
    parser.add_argument(
        "--keep-after",
        type=isoparse,
        default=(datetime.now(tz=UTC) - relativedelta(years=1)),
        nargs="?",
        help="Partitions with data before this date will be considered for cleanup",
    )
    return parser


@dataclass()
class TablePartition:
    name: str
    date: date
    granularity: Literal["year", "month", "day"] = field(default="year")

    @property
    def qualified_name(self):
        return self.name + self.date.strftime(granularity_output_format[self.granularity])


async def get_partitioned_tables(session: AsyncSession) -> dict[str, list[TablePartition]] | None:
    tables: dict[str, list[TablePartition]] = defaultdict(list)
    query = "select c.relname as table_name from pg_class c where c.relispartition = True and c.relkind = 'r' order by c.relname"  # noqa: E501
    query = query.format(partitioned_tables=PARTITIONED_TABLES)
    result = await session.execute(text(query))
    table_names = result.all()
    if not table_names:
        logger.info("There is no partitioned tables")
        return tables
    # parse tabel_names
    for (tabel_name,) in table_names:
        granularity: Literal["year", "month", "day"] = "year"
        match = re.search(PARTITION_GRANULARITY_PATERN, tabel_name)

        if not match:
            continue

        (name, year, month, day) = match.groups()  # type: ignore[union-attr]

        if name not in PARTITIONED_TABLES:
            continue
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
        tables[name].append(TablePartition(name=name, date=date(year, month, day), granularity=granularity))
    return tables


def get_partitions(tables: dict[str, list[TablePartition]], end_date: date) -> dict[str, list[TablePartition]]:
    tables_to_remove: dict[str, list[TablePartition]] = defaultdict(list)
    for table_name, partitions in tables.items():
        tables_to_remove[table_name].extend(partition for partition in partitions if partition.date < end_date)
    return tables_to_remove


def get_query(tables: dict[str, list[TablePartition]], query_type: str) -> list[str]:
    queries = []
    query_template = ""
    match query_type:
        case "detach":
            query_template = "ALTER TABLE {table_name} DETACH PARTITION {qualified_name};"
        case "remove":
            query_template = "DROP TABLE {qualified_name};"
        case "truncate":
            query_template = "TRUNCATE TABLE {qualified_name};"
    for table_name, partitions in tables.items():
        if not partitions:
            logger.info("No partitions of %s to %s", table_name, query_type)
            continue
        queries_per_table = [
            query_template.format(
                table_name=table_name,
                qualified_name=partition.qualified_name,
            )
            for partition in partitions
        ]
        queries.extend(queries_per_table)
    return queries


def show_removing_partitions(tables: dict[str, list[TablePartition]]):
    for table_name, partitions in tables.items():
        logger.info("Partitions to remove from table: %s", table_name)
        partitions_names = " ".join(
            [partition.qualified_name for partition in partitions],
        )
        logger.info(partitions_names)


async def detach_partitions(tables: dict[str, list[TablePartition]], session: AsyncSession):
    partitions_to_detach = get_query(tables, "detach")
    for query in partitions_to_detach:
        logger.debug("Detach partitions with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def remove_data(tables: dict[str, list[TablePartition]], session: AsyncSession):
    partitions_to_remove = get_query(tables, "remove")
    for query in partitions_to_remove:
        logger.debug("Remove table with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def truncate_data(tables: dict[str, list[TablePartition]], session: AsyncSession):
    partitions_to_truncate = get_query(tables, "truncate")
    for query in partitions_to_truncate:
        logger.debug("Truncate partition with query: %s", query)
        await session.execute(text(query))
        await session.commit()


async def main(args: list[str]) -> None:
    parser = get_parser()
    params = parser.parse_args(args)
    logger.info("Starting clean partition script with params: %s", params)
    end_date = params.keep_after.date()
    db_settings = DatabaseSettings()  # type: ignore[call-arg]
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        tables = await get_partitioned_tables(session)
        tables_to_remove = get_partitions(tables, end_date)  # type: ignore[arg-type]
        match params.command:
            case "dry_run":
                show_removing_partitions(tables_to_remove)
            case "detach_partitions":
                logger.info("Detach partitions starting from: %s", end_date)
                await detach_partitions(tables_to_remove, session)
            case "remove_data":
                logger.info("Remove data from partitions starting from: %s", end_date)
                await detach_partitions(tables_to_remove, session)
                await remove_data(tables_to_remove, session)
            case "truncate":
                logger.info("Truncate partitions starting from: %s", end_date)
                await truncate_data(tables_to_remove, session)
            case _:
                logger.error("No such command: %s", params.command)


if __name__ == "__main__":
    setup_logging(LoggingSettings())
    asyncio.run(main(sys.argv[1:]))
