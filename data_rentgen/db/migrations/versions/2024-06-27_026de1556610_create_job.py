# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create job

Revision ID: 026de1556610
Revises: c2c583123133
Create Date: 2024-06-27 19:11:39.585904

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "026de1556610"
down_revision = "c2c583123133"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple'::regconfig,  name || ' ' || (translate(name, '/.', '  ')))",
                persisted=True,
            ),
            nullable=False,
        ),
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
    op.create_index(op.f("ix__job__type"), "job", ["type"], unique=False)
    op.create_index(op.f("ix__job__search_vector"), "job", ["search_vector"], unique=False, postgresql_using="gin")


def downgrade() -> None:
    op.drop_index(op.f("ix__job__type"), table_name="job")
    op.drop_index(op.f("ix__job__name"), table_name="job")
    op.drop_index(op.f("ix__job__location_id"), table_name="job")
    op.drop_index(op.f("ix__job_search__vector"), table_name="job", postgresql_using="gin")
    op.drop_table("job")
