# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Change output.type from string to int

Revision ID: 7f46bf29bf48
Revises: 8267d8769ea0
Create Date: 2025-05-20 17:00:59.055050

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7f46bf29bf48"
down_revision = "8267d8769ea0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("LOCK TABLE output IN ACCESS EXCLUSIVE MODE;"))
    op.alter_column(
        "output",
        "type",
        existing_type=sa.String(length=32),
        type_=sa.SmallInteger(),
        nullable=False,
        postgresql_using="""
            CASE
                WHEN type = 'APPEND'
                    THEN 1
                WHEN type = 'CREATE'
                    THEN 2
                WHEN type = 'ALTER'
                    THEN 4
                WHEN type = 'RENAME'
                    THEN 8
                WHEN type = 'OVERWRITE'
                    THEN 16
                WHEN type = 'DROP'
                    THEN 32
                WHEN type = 'TRUNCATE'
                    THEN 64
                ELSE 0
            END
        """,
    )


def downgrade() -> None:
    op.execute(sa.text("LOCK TABLE job IN ACCESS EXCLUSIVE MODE;"))
    op.alter_column(
        "output",
        "type",
        existing_type=sa.SmallInteger(),
        type_=sa.String(length=32),
        nullable=False,
        postgresql_using="""
            CASE
                WHEN type = 1
                    THEN 'APPEND'
                WHEN type = 2
                    THEN 'CREATE'
                WHEN type = 4
                    THEN 'ALTER'
                WHEN type = 8
                    THEN 'RENAME'
                WHEN type = 16
                    THEN 'OVERWRITE'
                WHEN type = 32
                    THEN 'DROP'
                WHEN type = 64
                    THEN 'TRUNCATE'
                ELSE 0
            END
        """,
    )
