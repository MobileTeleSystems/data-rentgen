# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create user

Revision ID: f9a114951102
Revises: c324e48e0f71
Create Date: 2024-06-27 18:45:00.259710

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f9a114951102"
down_revision = "c324e48e0f71"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__user")),
    )
    op.create_index(op.f("ix__user__name"), "user", ["name"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix__user__name"), table_name="user")
    op.drop_table("user")
