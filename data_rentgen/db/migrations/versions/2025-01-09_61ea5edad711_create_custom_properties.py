# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create custom properties

Revision ID: 61ea5edad711
Revises: 3357c8f914aa
Create Date: 2025-01-09 12:12:04.489277

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "61ea5edad711"
down_revision = "3357c8f914aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "custom_properties",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__custom_properties")),
        sa.UniqueConstraint("name", name=op.f("uq__custom_properties__name")),
    )


def downgrade() -> None:
    op.drop_table("custom_properties")
