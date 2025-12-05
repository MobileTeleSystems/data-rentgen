# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Make dataset_tags foreign keys ON DELETE CASCADE

Revision ID: 5f52db7affd9
Revises: 17481d3b2466
Create Date: 2025-09-17 12:32:30.062717

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "5f52db7affd9"
down_revision = "17481d3b2466"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(op.f("fk__dataset_tags__tag_value_id__tag_value"), "dataset_tags", type_="foreignkey")
    op.drop_constraint(op.f("fk__dataset_tags__dataset_id__dataset"), "dataset_tags", type_="foreignkey")
    op.create_foreign_key(
        op.f("fk__dataset_tags__tag_value_id__tag_value"),
        "dataset_tags",
        "tag_value",
        ["tag_value_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("fk__dataset_tags__dataset_id__dataset"),
        "dataset_tags",
        "dataset",
        ["dataset_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("fk__dataset_tags__dataset_id__dataset"), "dataset_tags", type_="foreignkey")
    op.drop_constraint(op.f("fk__dataset_tags__tag_value_id__tag_value"), "dataset_tags", type_="foreignkey")
    op.create_foreign_key(
        op.f("fk__dataset_tags__dataset_id__dataset"),
        "dataset_tags",
        "dataset",
        ["dataset_id"],
        ["id"],
    )
    op.create_foreign_key(
        op.f("fk__dataset_tags__tag_value_id__tag_value"),
        "dataset_tags",
        "tag_value",
        ["tag_value_id"],
        ["id"],
    )
