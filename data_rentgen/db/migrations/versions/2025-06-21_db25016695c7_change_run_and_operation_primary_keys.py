# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Change run and operation primary keys to (id, created_at)

Revision ID: db25016695c7
Revises: 7f46bf29bf48
Create Date: 2025-06-21 20:26:28.058063

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "db25016695c7"
down_revision = "7f46bf29bf48"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # prevent inserting new rows into table without primary key
    op.execute(sa.text("LOCK TABLE operation IN EXCLUSIVE MODE;"))
    op.execute(sa.text("LOCK TABLE run IN EXCLUSIVE MODE;"))

    # in most cases, we filter runs by id, so it should be in the beginning of the index
    op.drop_constraint("pk__operation", "operation", type_="primary")
    op.create_primary_key("pk__operation", "operation", ["id", "created_at"])

    op.drop_constraint("pk__run", "run", type_="primary")
    op.create_primary_key("pk__run", "run", ["id", "created_at"])


def downgrade() -> None:
    op.execute(sa.text("LOCK TABLE operation IN EXCLUSIVE MODE;"))
    op.execute(sa.text("LOCK TABLE run IN EXCLUSIVE MODE;"))

    op.drop_constraint("pk__operation", "operation", type_="primary")
    op.create_primary_key("pk__operation", "operation", ["created_at", "id"])

    op.drop_constraint("pk__run", "run", type_="primary")
    op.create_primary_key("pk__run", "run", ["created_at", "id"])
