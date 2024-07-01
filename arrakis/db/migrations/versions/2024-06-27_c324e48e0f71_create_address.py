# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create address

Revision ID: c324e48e0f71
Revises: e730f34b9893
Create Date: 2024-06-27 18:44:50.259710

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c324e48e0f71"
down_revision = "e730f34b9893"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "address",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("url", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
            name=op.f("fk__address__location_id__location"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__address")),
        sa.UniqueConstraint("location_id", "url", name=op.f("uq__address__location_id_url")),
    )
    op.create_index(op.f("ix__address__location_id"), "address", ["location_id"], unique=False)
    op.create_index(op.f("ix__address__url"), "address", ["url"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__address__url"), table_name="address")
    op.drop_index(op.f("ix__address__location_id"), table_name="address")
    op.drop_table("address")