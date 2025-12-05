# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add tags for datasets

Revision ID: b8f1e422143d
Revises: def0be789a48
Create Date: 2025-07-14 11:10:27.538071

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b8f1e422143d"
down_revision = "def0be789a48"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tag",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__tag")),
    )
    op.create_table(
        "tag_value",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("tag_id", sa.BigInteger(), nullable=False),
        sa.Column("value", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["tag_id"], ["tag.id"], name=op.f("fk__tag_value__tag_id__tag"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__tag_value")),
    )
    op.create_index(op.f("ix__tag_value__tag_id"), "tag_value", ["tag_id"], unique=False)
    op.create_table(
        "dataset_tags",
        sa.Column("dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("tag_value_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"], name=op.f("fk__dataset_tags__dataset_id__dataset")),
        sa.ForeignKeyConstraint(
            ["tag_value_id"],
            ["tag_value.id"],
            name=op.f("fk__dataset_tags__tag_value_id__tag_value"),
        ),
        sa.PrimaryKeyConstraint("dataset_id", "tag_value_id", name=op.f("pk__dataset_tags")),
    )


def downgrade() -> None:
    op.drop_table("dataset_tags")
    op.drop_index(op.f("ix__tag_value__tag_id"), table_name="tag_value")
    op.drop_table("tag_value")
    op.drop_table("tag")
