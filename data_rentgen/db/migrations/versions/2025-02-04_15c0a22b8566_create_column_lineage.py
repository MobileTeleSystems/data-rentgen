# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create column lineage

Revision ID: 15c0a22b8566
Revises: f017d4c58658
Create Date: 2025-02-04 11:30:21.190161

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "15c0a22b8566"
down_revision = "f017d4c58658"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_column_relation",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("fingerprint", sa.UUID(), nullable=False),
        sa.Column("source_column", sa.String(length=255), nullable=False),
        sa.Column("target_column", sa.String(length=255), nullable=True),
        sa.Column("type", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__dataset_column_relation")),
    )
    op.create_index(
        op.f("ix__dataset_column_relation__fingerprint_source_column_target_column"),
        "dataset_column_relation",
        ["fingerprint", "source_column", sa.text("coalesce(target_column, '')")],
        unique=True,
    )

    op.create_table(
        "column_lineage",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("operation_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("source_dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("target_dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("fingerprint", sa.UUID(), nullable=False),
        sa.PrimaryKeyConstraint("created_at", "id", name=op.f("pk__column_lineage")),
        postgresql_partition_by="RANGE (created_at)",
    )
    op.create_index(op.f("ix__column_lineage__job_id"), "column_lineage", ["job_id"], unique=False)
    op.create_index(op.f("ix__column_lineage__operation_id"), "column_lineage", ["operation_id"], unique=False)
    op.create_index(op.f("ix__column_lineage__run_id"), "column_lineage", ["run_id"], unique=False)
    op.create_index(
        op.f("ix__column_lineage__source_dataset_id"),
        "column_lineage",
        ["source_dataset_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix__column_lineage__target_dataset_id"),
        "column_lineage",
        ["target_dataset_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix__column_lineage__target_dataset_id"), table_name="column_lineage")
    op.drop_index(op.f("ix__column_lineage__source_dataset_id"), table_name="column_lineage")
    op.drop_index(op.f("ix__column_lineage__run_id"), table_name="column_lineage")
    op.drop_index(op.f("ix__column_lineage__operation_id"), table_name="column_lineage")
    op.drop_index(op.f("ix__column_lineage__job_id"), table_name="column_lineage")
    op.drop_table("column_lineage")

    op.drop_index(
        op.f("ix__dataset_column_relation__fingerprint_source_column_target_column"),
        table_name="dataset_column_relation",
    )
    op.drop_table("dataset_column_relation")
