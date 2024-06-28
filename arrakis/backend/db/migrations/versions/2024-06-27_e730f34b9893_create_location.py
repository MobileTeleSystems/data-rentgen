# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create location

Revision ID: e730f34b9893
Revises:
Create Date: 2024-06-27 18:44:49.259710

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e730f34b9893"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "location",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__location")),
    )
    op.create_index(op.f("ix__location__type_name"), "location", ["type", "name"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix__location__type_name"), table_name="location")
    op.drop_table("location")
