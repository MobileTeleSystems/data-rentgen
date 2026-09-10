# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Change fillfactor

Revision ID: 662a0942860f
Revises: 484cee706cc2
Create Date: 2026-09-10 13:10:56.164830

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "662a0942860f"
down_revision = "484cee706cc2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # These tables have high number of UPDATEs, so we need extra space for new tuples
    op.execute("ALTER TABLE job SET (fillfactor = 70)")
    op.execute("ALTER INDEX ix__job__parent_job_id SET (fillfactor = 70)")
    op.execute("ALTER TABLE job_dependency SET (fillfactor = 70)")


def downgrade() -> None:
    op.execute("ALTER TABLE job_dependency SET (fillfactor = 100)")
    op.execute("ALTER INDEX ix__job__parent_job_id SET (fillfactor = 90)")
    op.execute("ALTER TABLE job SET (fillfactor = 100)")
