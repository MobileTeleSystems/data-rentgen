# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Rename table dataset_tags to dataset_tag

Revision ID: f1c76c422433
Revises: 8e8891273099
Create Date: 2026-01-21 11:27:20.255770

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f1c76c422433"
down_revision = "8e8891273099"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("dataset_tags", "dataset_tag")
    op.execute("ALTER INDEX ix__dataset_tags__tag_value_id RENAME TO ix__dataset_tag__tag_value_id")


def downgrade() -> None:
    op.rename_table("dataset_tag", "dataset_tags")
    op.execute("ALTER INDEX ix__dataset_tag__tag_value_id RENAME TO ix__dataset_tags__tag_value_id")
