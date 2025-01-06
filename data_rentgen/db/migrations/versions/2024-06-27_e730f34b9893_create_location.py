# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create location

Revision ID: e730f34b9893
Revises:
Create Date: 2024-06-27 18:44:49.259710

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e730f34b9893"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "location",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("external_id", sa.String(), nullable=True),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', ' ')) || ' ' || type)",
                persisted=True,
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__location")),
    )
    op.create_index(op.f("ix__location__type_name"), "location", ["type", "name"], unique=True)
    op.create_index(
        op.f("ix__location__search_vector"),
        "location",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(op.f("ix__location__type_name"), table_name="location")
    op.drop_index(op.f("ix__location__search_vector"), table_name="location", postgresql_using="gin")
    op.drop_table("location")
