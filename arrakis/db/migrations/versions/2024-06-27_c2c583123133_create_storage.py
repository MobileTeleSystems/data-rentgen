# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create storage

Revision ID: c2c583123133
Revises: 81153f43e276
Create Date: 2024-06-27 19:10:11.686319

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2c583123133"
down_revision = "81153f43e276"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "storage",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("namespace", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
            name=op.f("fk__storage__location_id__location"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__storage")),
    )


def downgrade() -> None:
    op.drop_index(op.f("ix__storage__location_id_namespace"), table_name="storage")
    op.drop_index(op.f("ix__storage__location_id"), table_name="storage")
    op.drop_table("storage")
