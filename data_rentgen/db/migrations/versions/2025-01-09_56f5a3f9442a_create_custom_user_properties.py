# SPDX-FileCopyrightText: 2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create custom user properties

Revision ID: 56f5a3f9442a
Revises: 61ea5edad711
Create Date: 2025-01-09 13:25:39.718162

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE, ExcludeConstraint

# revision identifiers, used by Alembic.
revision = "56f5a3f9442a"
down_revision = "61ea5edad711"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist;"))
    op.create_table(
        "custom_user_properties",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("property_id", sa.BigInteger, nullable=False),
        sa.Column("value", JSONB, nullable=False),
        sa.Column(
            "during",
            TSTZRANGE,
            server_default=sa.text("tstzrange(now(), NULL, '[)')"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__custom_user_properties")),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user.id"],
            name=op.f("fk__custom_user_properties__user_id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["property_id"],
            ["custom_properties.id"],
            name=op.f("fk__custom_user_properties__custom_properties_id"),
            ondelete="CASCADE",
        ),
        ExcludeConstraint(
            ("user_id", "="),
            ("property_id", "="),
            ("during", "&&"),
            name="uq__custom_user_properties__user_id_property_id_during",
            using="gist",
        ),
    )
    op.create_index(
        op.f("ix__custom_user_properties__user_id"),
        "custom_user_properties",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix__custom_user_properties__property_id"),
        "custom_user_properties",
        ["property_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix__custom_user_properties__user_id"),
        "custom_user_properties",
    )
    op.drop_index(
        op.f("ix__custom_user_properties__property_id"),
        "custom_user_properties",
    )
    op.drop_table("custom_user_properties")
