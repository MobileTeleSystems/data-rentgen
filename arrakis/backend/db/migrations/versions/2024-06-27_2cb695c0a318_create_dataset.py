# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create dataset

Revision ID: 2cb695c0a318
Revises: c2c583123133
Create Date: 2024-06-27 19:10:39.585904

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2cb695c0a318"
down_revision = "c2c583123133"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("storage_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("format", sa.String(length=64), nullable=True),
        sa.Column("alias_for_dataset_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["alias_for_dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset__alias_for_dataset_id__dataset"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["storage_id"],
            ["storage.id"],
            name=op.f("fk__dataset__storage_id__storage"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__dataset")),
        sa.UniqueConstraint("storage_id", "name", name=op.f("uq__dataset__storage_id_name")),
    )
    op.create_index(op.f("ix__dataset__alias_for_dataset_id"), "dataset", ["alias_for_dataset_id"], unique=False)
    op.create_index(op.f("ix__dataset__name"), "dataset", ["name"], unique=False)
    op.create_index(op.f("ix__dataset__storage_id"), "dataset", ["storage_id"], unique=False)
    op.create_index(op.f("ix__storage__location_id"), "storage", ["location_id"], unique=False)
    op.create_index(op.f("ix__storage__location_id_namespace"), "storage", ["location_id", "namespace"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix__storage__location_id_namespace"), table_name="storage")
    op.drop_index(op.f("ix__storage__location_id"), table_name="storage")
    op.drop_index(op.f("ix__dataset__storage_id"), table_name="dataset")
    op.drop_index(op.f("ix__dataset__name"), table_name="dataset")
    op.drop_index(op.f("ix__dataset__alias_for_dataset_id"), table_name="dataset")
    op.drop_table("dataset")
