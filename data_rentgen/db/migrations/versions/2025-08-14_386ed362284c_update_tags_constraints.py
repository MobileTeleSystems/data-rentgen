# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Update tags constraints

Revision ID: 386ed362284c
Revises: 534b8fffbe5e
Create Date: 2025-08-14 12:18:26.599005

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "386ed362284c"
down_revision = "534b8fffbe5e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("tag_value", "value", existing_type=sa.String(64), type_=sa.String(256))
    op.create_unique_constraint(op.f("uq__tag_value__tag_id_value"), "tag_value", ["tag_id", "value"])


def downgrade() -> None:
    op.alter_column("tag_value", "value", existing_type=sa.String(256), type_=sa.String(64))
    op.drop_constraint(op.f("uq__tag_value__tag_id_value"), "tag_value", type_="unique")
