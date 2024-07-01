# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create job

Revision ID: 026de1556610
Revises: 2cb695c0a318
Create Date: 2024-06-27 19:11:39.585904

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "026de1556610"
down_revision = "2cb695c0a318"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
            name=op.f("fk__job__location_id__location"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__job")),
        sa.UniqueConstraint("location_id", "name", name=op.f("uq__job__location_id_name")),
    )
    op.create_index(op.f("ix__job__location_id"), "job", ["location_id"], unique=False)
    op.create_index(op.f("ix__job__name"), "job", ["name"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__job__name"), table_name="job")
    op.drop_index(op.f("ix__job__location_id"), table_name="job")
    op.drop_table("job")
