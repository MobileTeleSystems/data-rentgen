# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Connection, MetaData, create_engine

from arrakis.backend.db.models import Base

pytestmark = [pytest.mark.backend]


def get_diff_db_metadata(connection: Connection, metadata: MetaData):
    migration_ctx = MigrationContext.configure(connection)
    return compare_metadata(context=migration_ctx, metadata=metadata)


def test_migrations_up_to_date(empty_db_url: str, run_migrations):
    with create_engine(empty_db_url).connect() as connection:
        diff = get_diff_db_metadata(connection, metadata=Base.metadata)
    assert not diff
