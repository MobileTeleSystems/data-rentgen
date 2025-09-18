# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add index for dataset_tags.tag_value_id

Revision ID: 102502e85b2d
Revises: 5f52db7affd9
Create Date: 2025-09-18 11:34:07.261497

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "102502e85b2d"
down_revision = "5f52db7affd9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix__dataset_tags__tag_value_id", "dataset_tags", ["tag_value_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix__dataset_tags__tag_value_id", table_name="dataset_tags")
