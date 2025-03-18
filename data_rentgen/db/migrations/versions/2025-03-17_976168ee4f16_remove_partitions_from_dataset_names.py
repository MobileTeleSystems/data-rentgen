# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Remove partitions from dataset names

Revision ID: 976168ee4f16
Revises: 15c0a22b8566
Create Date: 2025-03-17 18:28:24.545234

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "976168ee4f16"
down_revision = "15c0a22b8566"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create a Map old_id -> new_id
    op.execute(sa.text("CREATE TEMP TABLE dataset_migration (id SERIAL PRIMARY KEY, old_id INT, new_id INT);"))
    # Add new rows into dataset with 'short' name if such names doesn't exists
    op.execute(
        sa.text(
            """
            INSERT INTO dataset (name, location_id, format)
            SELECT DISTINCT REGEXP_REPLACE(name, '/[^/]*=[^/]*.*', ''), location_id, format
            FROM dataset WHERE regexp_match(name, '/[^/]*=[^/]*.*') is not null
            ON CONFLICT DO NOTHING;
            """,
        ),
    )
    # Fill Map table
    op.execute(
        sa.text(
            """
            WITH new_datasets as (
                SELECT id, name
                FROM dataset WHERE name in (
                    SELECT distinct regexp_replace(name, '/[^/]*=[^/]*.*', '')
                    FROM dataset WHERE regexp_match(name, '/[^/]*=[^/]*.*') is not null)
            )
            INSERT INTO dataset_migration (old_id, new_id)
            SELECT d1.id, d2.id
            FROM dataset AS d1
            JOIN new_datasets AS d2 ON REGEXP_REPLACE(d1.name, '/[^/]*=[^/]*.*', '') = d2.name;
            """,
        ),
    )
    # Update dataset_ids in related tables
    op.execute(
        sa.text(
            """
            UPDATE output o
            SET dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE o.dataset_id = dm.old_id;
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE input o
            SET dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE o.dataset_id = dm.old_id;
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE column_lineage cl
            SET source_dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE cl.source_dataset_id = dm.old_id;
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE column_lineage cl
            SET target_dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE cl.target_dataset_id = dm.old_id;
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE dataset_symlink ds
            SET from_dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE ds.from_dataset_id = dm.old_id;
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE dataset_symlink ds
            SET to_dataset_id = dm.new_id
            FROM dataset_migration dm
            WHERE ds.to_dataset_id = dm.old_id
            """,
        ),
    )
    # Delete row with old datasets
    op.execute(
        sa.text(
            """
            DELETE FROM dataset
            WHERE id IN (SELECT old_id FROM dataset_migration);
            """,
        ),
    )
    # Remove duplicates from input and output
    op.execute(
        sa.text(
            """
            WITH duplicates as (
                SELECT id, row_number() over(partition by dataset_id, operation_id order by created_at desc) as row_num
                FROM input
            )
            DELETE from input where id in (select id from duplicates where row_num > 1);
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            WITH duplicates as (
                SELECT id, row_number() over(partition by dataset_id, operation_id order by created_at desc) as row_num
                FROM output
            )
            DELETE from output where id in (select id from duplicates where row_num > 1);
            """,
        ),
    )


def downgrade() -> None:
    # This migration is irreversible
    pass
