# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create interaction

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
        "interaction",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operation_id", sa.BigInteger(), nullable=False),
        sa.Column("dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=255), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("schema_id", sa.BigInteger(), nullable=True),
        sa.Column("connect_as_user_id", sa.BigInteger(), nullable=True),
        sa.Column("num_bytes", sa.BigInteger(), nullable=True),
        sa.Column("num_rows", sa.BigInteger(), nullable=True),
        sa.Column("num_files", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("started_at", "id", name=op.f("pk__interaction")),
        postgresql_partition_by="RANGE (started_at)",
    )
    op.create_index(op.f("ix__interaction__connect_as_user_id"), "interaction", ["connect_as_user_id"], unique=False)
    op.create_index(op.f("ix__interaction__dataset_id"), "interaction", ["dataset_id"], unique=False)
    op.create_index(op.f("ix__interaction__operation_id"), "interaction", ["operation_id"], unique=False)
    op.create_index(op.f("ix__interaction__schema_id"), "interaction", ["schema_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__interaction__schema_id"), table_name="interaction")
    op.drop_index(op.f("ix__interaction__operation_id"), table_name="interaction")
    op.drop_index(op.f("ix__interaction__dataset_id"), table_name="interaction")
    op.drop_index(op.f("ix__interaction__connect_as_user_id"), table_name="interaction")
    op.drop_table("interaction")
