# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create operation

Revision ID: 0b9aac68402b
Revises: 5f8fff06dd76
Create Date: 2024-06-27 19:15:50.909604

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0b9aac68402b"
down_revision = "5f8fff06dd76"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operation",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.SmallInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("group", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("created_at", "id", name=op.f("pk__operation")),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(op.f("ix__operation__run_id"), "operation", ["run_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__operation__run_id"), table_name="operation")
    op.drop_table("operation")
