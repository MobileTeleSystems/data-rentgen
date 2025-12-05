# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create dataset

Revision ID: 2cb695c0a318
Revises: 81153f43e276
Create Date: 2024-06-27 19:10:39.585904

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "2cb695c0a318"
down_revision = "81153f43e276"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("format", sa.String(length=32), nullable=True),
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed(
                "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', '  ')))",
                persisted=True,
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["location_id"],
            ["location.id"],
            name=op.f("fk__dataset__location_id__location"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__dataset")),
        sa.UniqueConstraint("location_id", "name", name=op.f("uq__dataset__location_id_name")),
    )
    op.create_index(op.f("ix__dataset__name"), "dataset", ["name"], unique=False)
    op.create_index(op.f("ix__dataset__location_id"), "dataset", ["location_id"], unique=False)
    op.create_index(
        op.f("ix__dataset__search_vector"),
        "dataset",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(op.f("ix__dataset__location_id"), table_name="dataset")
    op.drop_index(op.f("ix__dataset__name"), table_name="dataset")
    op.drop_index(op.f("ix__dataset__search_vector"), table_name="dataset", postgresql_using="gin")
    op.drop_table("dataset")
