# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create runner

Revision ID: ecc373863e9c
Revises: 026de1556610
Create Date: 2024-06-27 19:13:46.056979

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ecc373863e9c"
down_revision = "026de1556610"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runner",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=False),
        sa.Column("version", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
            name=op.f("fk__runner__location_id__location"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__runner")),
        sa.UniqueConstraint("location_id", "type", "version", name=op.f("uq__runner__location_id_type_version")),
    )
    op.create_index(op.f("ix__runner__location_id"), "runner", ["location_id"], unique=False)
    op.create_index(op.f("ix__runner__type"), "runner", ["type"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__runner__type"), table_name="runner")
    op.drop_index(op.f("ix__runner__location_id"), table_name="runner")
    op.drop_table("runner")
