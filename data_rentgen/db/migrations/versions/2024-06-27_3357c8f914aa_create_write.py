# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create output

Revision ID: 3357c8f914aa
Revises: 412d5fbdb362
Create Date: 2024-06-27 19:25:51.909604

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3357c8f914aa"
down_revision = "412d5fbdb362"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "output",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("operation_id", sa.UUID(), nullable=False),
        sa.Column("dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("schema_id", sa.BigInteger(), nullable=True),
        sa.Column("num_bytes", sa.BigInteger(), nullable=True),
        sa.Column("num_rows", sa.BigInteger(), nullable=True),
        sa.Column("num_files", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("created_at", "id", name=op.f("pk__output")),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(op.f("ix__output__dataset_id"), "output", ["dataset_id"], unique=False)
    op.create_index(op.f("ix__output__operation_id"), "output", ["operation_id"], unique=False)
    op.create_index(op.f("ix__output__run_id"), "output", ["run_id"], unique=False)
    op.create_index(op.f("ix__output__job_id"), "output", ["job_id"], unique=False)
    op.create_index(op.f("ix__output__schema_id"), "output", ["schema_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__output__schema_id"), table_name="output")
    op.drop_index(op.f("ix__output__job_id"), table_name="output")
    op.drop_index(op.f("ix__output__run_id"), table_name="output")
    op.drop_index(op.f("ix__output__operation_id"), table_name="output")
    op.drop_index(op.f("ix__output__dataset_id"), table_name="output")
    op.drop_table("output")
