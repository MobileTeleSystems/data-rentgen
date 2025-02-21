# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create input

Revision ID: 412d5fbdb362
Revises: 0b9aac68402b
Create Date: 2024-06-27 19:25:50.909604

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "412d5fbdb362"
down_revision = "0b9aac68402b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "input",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("operation_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("schema_id", sa.BigInteger(), nullable=True),
        sa.Column("num_bytes", sa.BigInteger(), nullable=True),
        sa.Column("num_rows", sa.BigInteger(), nullable=True),
        sa.Column("num_files", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("created_at", "id", name=op.f("pk__input")),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(op.f("ix__input__dataset_id"), "input", ["dataset_id"], unique=False)
    op.create_index(op.f("ix__input__operation_id"), "input", ["operation_id"], unique=False)
    op.create_index(op.f("ix__input__run_id"), "input", ["run_id"], unique=False)
    op.create_index(op.f("ix__input__job_id"), "input", ["job_id"], unique=False)
    op.create_index(op.f("ix__input__schema_id"), "input", ["schema_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__input__schema_id"), table_name="input")
    op.drop_index(op.f("ix__input__job_id"), table_name="input")
    op.drop_index(op.f("ix__input__run_id"), table_name="input")
    op.drop_index(op.f("ix__input__operation_id"), table_name="input")
    op.drop_index(op.f("ix__input__dataset_id"), table_name="input")
    op.drop_table("input")
