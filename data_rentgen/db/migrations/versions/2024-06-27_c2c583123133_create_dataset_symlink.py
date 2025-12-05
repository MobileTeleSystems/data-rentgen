# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create dataset_symlink

Revision ID: c2c583123133
Revises: 2cb695c0a318
Create Date: 2024-06-27 19:10:11.686319

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2c583123133"
down_revision = "2cb695c0a318"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_symlink",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("from_dataset_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("to_dataset_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["from_dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset_symlink__from_dataset_id__dataset"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["to_dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset_symlink__to_dataset_id__dataset"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__dataset_symlink")),
        sa.UniqueConstraint(
            "from_dataset_id",
            "to_dataset_id",
            name=op.f("uq__dataset_symlink__from_dataset_id_to_dataset_id"),
        ),
    )
    op.create_index(op.f("ix__dataset_symlink__to_dataset_id"), "dataset_symlink", ["to_dataset_id"], unique=False)
    op.create_index(op.f("ix__dataset_symlink__from_dataset_id"), "dataset_symlink", ["from_dataset_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__dataset_symlink__from_dataset_id"), table_name="dataset_symlink")
    op.drop_index(op.f("ix__dataset_symlink__to_dataset_id"), table_name="dataset_symlink")
    op.drop_table("dataset_symlink")
