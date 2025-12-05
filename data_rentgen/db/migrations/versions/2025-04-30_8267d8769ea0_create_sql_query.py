# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create sql_query

Revision ID: 8267d8769ea0
Revises: 2d2fe3f2f348
Create Date: 2025-04-30 11:48:18.467843

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8267d8769ea0"
down_revision = "2d2fe3f2f348"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sql_query",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("fingerprint", sa.UUID(), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__sql_query")),
    )
    op.create_index(op.f("ix__sql_query__fingerprint"), "sql_query", ["fingerprint"], unique=True)

    op.add_column(
        "operation",
        sa.Column("sql_query_id", sa.BigInteger, nullable=True),
    )
    op.create_index(op.f("ix__operation__sql_query_id"), "operation", ["sql_query_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__operation__sql_query_id"), table_name="operation")
    op.drop_index(op.f("ix__sql_query__fingerprint"), table_name="sql_query")
    op.drop_table("sql_query")
