# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add job_type

Revision ID: 2d2fe3f2f348
Revises: 976168ee4f16
Create Date: 2025-04-25 15:09:17.556969

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2d2fe3f2f348"
down_revision = "976168ee4f16"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_type",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__job_type")),
        sa.UniqueConstraint("type", name=op.f("uq__job_type__type")),
    )
    op.create_index(op.f("ix__job_type__type"), "job_type", ["type"], unique=False)

    op.execute(
        sa.text(
            """
        INSERT INTO
            job_type (id, type)
        VALUES
            (0, 'UNKNOWN'),
            (1, 'SPARK_APPLICATION'),
            (2, 'AIRFLOW_DAG'),
            (3, 'AIRFLOW_TASK');
        """,
        ),
    )
    op.execute(sa.text("ALTER SEQUENCE job_type_id_seq RESTART WITH 4;"))

    op.execute(sa.text("LOCK TABLE job IN ACCESS EXCLUSIVE MODE;"))
    op.drop_index("ix__job__type", table_name="job")
    op.alter_column(
        "job",
        "type",
        new_column_name="type_id",
        existing_type=sa.String(length=32),
        type_=sa.BigInteger(),
        nullable=False,
        postgresql_using="""
            CASE
                WHEN type = 'SPARK_APPLICATION'
                    THEN 1
                WHEN type = 'AIRFLOW_DAG'
                    THEN 2
                WHEN type = 'AIRFLOW_TASK'
                    THEN 3
                ELSE 0
            END
        """,
    )
    op.create_index(op.f("ix__job__type_id"), "job", ["type_id"], unique=False)
    op.create_foreign_key(
        op.f("fk__job__type_id__job_type"),
        "job",
        "job_type",
        ["type_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.execute(sa.text("LOCK TABLE job IN ACCESS EXCLUSIVE MODE;"))
    op.drop_constraint(op.f("fk__job__type_id__job_type"), "job", type_="foreignkey")
    op.drop_index(op.f("ix__job__type_id"), table_name="job")
    op.alter_column(
        "job",
        "type_id",
        new_column_name="type",
        existing_type=sa.BigInteger(),
        type_=sa.String(length=32),
        nullable=False,
    )
    op.execute(
        sa.text(
            """
            UPDATE job
            SET type = (SELECT job_type.type FROM job_type WHERE job_type.id = job.type);
            """,
        ),
    )
    op.create_index("ix__job__type", "job", ["type"], unique=False)

    op.drop_index(op.f("ix__job_type__type"), table_name="job_type")
    op.drop_table("job_type")
