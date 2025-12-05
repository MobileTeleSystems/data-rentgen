# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Drop dataset.format

Revision ID: def0be789a48
Revises: db25016695c7
Create Date: 2025-07-04 22:38:56.153647

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "def0be789a48"
down_revision = "db25016695c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("dataset", "format")


def downgrade() -> None:
    op.add_column("dataset", sa.Column("format", sa.VARCHAR(length=32), autoincrement=False, nullable=True))
