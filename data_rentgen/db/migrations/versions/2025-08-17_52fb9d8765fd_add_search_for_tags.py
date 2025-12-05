# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add search for tags

Revision ID: 52fb9d8765fd
Revises: 386ed362284c
Create Date: 2025-08-17 16:46:39.203240

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "52fb9d8765fd"
down_revision = "386ed362284c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(op.f("uq__tag__name"), "tag", ["name"])
    op.add_column(
        "tag",
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '.', '  ')))",
                persisted=True,
            ),
            nullable=False,
        ),
    )
    op.create_index("ix__tag__search_vector", "tag", ["search_vector"], unique=False, postgresql_using="gin")
    op.add_column(
        "tag_value",
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple'::regconfig, value || ' ' || (translate(value, '.', '  ')))",
                persisted=True,
            ),
            nullable=False,
        ),
    )
    op.create_index(
        "ix__tag_value__search_vector",
        "tag_value",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix__tag_value__search_vector", table_name="tag_value", postgresql_using="gin")
    op.drop_column("tag_value", "search_vector")
    op.drop_index("ix__tag__search_vector", table_name="tag", postgresql_using="gin")
    op.drop_column("tag", "search_vector")
    op.drop_constraint(op.f("uq__tag__name"), "tag", type_="unique")
