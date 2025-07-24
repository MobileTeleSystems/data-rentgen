# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add personal_token

Revision ID: 534b8fffbe5e
Revises: 4655ed1b9501
Create Date: 2025-07-23 12:57:11.514447

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, ExcludeConstraint

# revision identifiers, used by Alembic.
revision = "534b8fffbe5e"
down_revision = "4655ed1b9501"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "personal_token",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("scopes", JSONB(), nullable=False),
        sa.Column("since", sa.Date(), nullable=False),
        sa.Column("until", sa.Date(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        ExcludeConstraint(
            (sa.column("user_id"), "="),
            (sa.func.daterange(sa.column("since"), sa.column("until"), "[]"), "&&"),
            (sa.column("name"), "="),
            where=sa.text("revoked_at IS NULL"),
            using="gist",
            name="uq__personal_token__user_id_since_until_name",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
            name=op.f("fk__personal_token__user_id__user"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__personal_token")),
    )


def downgrade() -> None:
    op.drop_table("personal_token")
