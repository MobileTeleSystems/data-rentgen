# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Include external_id to location search index

Revision ID: 85592fce8fb0
Revises: 17481d3b2466
Create Date: 2025-09-16 11:54:45.571184

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "85592fce8fb0"
down_revision = "52fb9d8765fd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("ALTER TABLE location DROP COLUMN search_vector"))
    op.execute(
        sa.text(
            """
            ALTER TABLE location ADD COLUMN search_vector tsvector NOT NULL
            GENERATED ALWAYS AS (
                to_tsvector(
                    'simple'::regconfig,
                    type || ' ' ||
                    name || ' ' || (translate(name, '/.', ' ')) || ' ' ||
                    COALESCE(external_id, ''::text) || ' ' || (translate(COALESCE(external_id, ''::text), '/.', ' '))
                )
            ) STORED
            """,
        ),
    )
    op.create_index(
        op.f("ix__location__search_vector"),
        "location",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.execute(sa.text("ALTER TABLE location DROP COLUMN search_vector"))
    op.execute(
        sa.text(
            """
            ALTER TABLE location ADD COLUMN search_vector tsvector NOT NULL
            GENERATED ALWAYS AS (
                to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', ' ')) || ' ' || type)
            ) STORED
            """,
        ),
    )
    op.create_index(
        op.f("ix__location__search_vector"),
        "location",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )
