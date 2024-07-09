# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create run

Revision ID: 5f8fff06dd76
Revises: ecc373863e9c
Create Date: 2024-06-27 19:14:50.909604

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5f8fff06dd76"
down_revision = "ecc373863e9c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "run",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("parent_run_id", sa.UUID(), nullable=True),
        sa.Column("status", sa.String(length=255), nullable=False),
        sa.Column("runner_id", sa.BigInteger(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("attempt", sa.String(length=64), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("log_url", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_by_user_id", sa.BigInteger(), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_reason", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("created_at", "id", name=op.f("pk__run")),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(op.f("ix__run__job_id"), "run", ["job_id"], unique=False)
    op.create_index(op.f("ix__run__parent_run_id"), "run", ["parent_run_id"], unique=False)
    op.create_index(op.f("ix__run__runner_id"), "run", ["runner_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__run__runner_id"), table_name="run")
    op.drop_index(op.f("ix__run__parent_run_id"), table_name="run")
    op.drop_index(op.f("ix__run__job_id"), table_name="run")
    op.drop_table("run")
