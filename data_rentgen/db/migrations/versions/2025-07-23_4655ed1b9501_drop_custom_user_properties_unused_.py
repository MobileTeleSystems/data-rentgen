# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Drop custom_user_properties unused indexes

Revision ID: 4655ed1b9501
Revises: b8f1e422143d
Create Date: 2025-07-23 12:55:28.351325

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4655ed1b9501"
down_revision = "b8f1e422143d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ExcludeConstraint already created index under the hood, on the same columns
    op.drop_index(op.f("ix__custom_user_properties__property_id"), table_name="custom_user_properties")
    op.drop_index(op.f("ix__custom_user_properties__user_id"), table_name="custom_user_properties")


def downgrade() -> None:
    op.create_index(
        op.f("ix__custom_user_properties__user_id"),
        table_name="custom_user_properties",
        columns=["user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix__custom_user_properties__property_id"),
        table_name="custom_user_properties",
        columns=["property_id"],
        unique=False,
    )
