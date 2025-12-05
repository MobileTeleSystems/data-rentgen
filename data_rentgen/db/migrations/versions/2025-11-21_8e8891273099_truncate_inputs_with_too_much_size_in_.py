# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Truncate inputs with too much size in bytes

Revision ID: 8e8891273099
Revises: 102502e85b2d
Create Date: 2025-11-21 18:28:52.279644

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8e8891273099"
down_revision = "102502e85b2d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # https://github.com/OpenLineage/OpenLineage/pull/4165
    op.execute(sa.text("UPDATE input SET num_bytes = NULL WHERE num_bytes >= 9223372036854775807"))


def downgrade() -> None:
    pass
