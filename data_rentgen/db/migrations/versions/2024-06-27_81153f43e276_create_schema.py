# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create schema

Revision ID: 81153f43e276
Revises: f9a114951102
Create Date: 2024-06-27 18:45:49.259710

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "81153f43e276"
down_revision = "f9a114951102"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "schema",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("digest", sa.UUID(), nullable=False),
        sa.Column("fields", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__schema")),
    )
    op.create_index(op.f("ix__schema__digest"), "schema", ["digest"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix__schema__digest"), table_name="schema")
    op.drop_table("schema")
