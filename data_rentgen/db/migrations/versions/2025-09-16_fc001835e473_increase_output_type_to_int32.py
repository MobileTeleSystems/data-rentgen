# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Increase output.type to int32

Revision ID: fc001835e473
Revises: 85592fce8fb0
Create Date: 2025-09-15 18:53:32.392011

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fc001835e473"
down_revision = "85592fce8fb0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "output",
        "type",
        existing_type=sa.SMALLINT(),
        type_=sa.INTEGER(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "output",
        "type",
        existing_type=sa.INTEGER(),
        type_=sa.SMALLINT(),
        existing_nullable=False,
    )
