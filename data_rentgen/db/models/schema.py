# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from uuid import UUID

from sqlalchemy import JSON
from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base
from data_rentgen.db.utils.fields import get_fields_digest


def _get_digest(context: DefaultExecutionContext) -> UUID:
    fields = context.get_current_parameters()["fields"]
    return get_fields_digest(fields)


class Schema(Base):
    __tablename__ = "schema"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    digest: Mapped[UUID] = mapped_column(
        SQL_UUID,
        nullable=False,
        index=True,
        unique=True,
        default=_get_digest,
        doc="Schema SHA-1 digest based on fields content. Currently this is in form of UUID",
    )
    fields: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        doc="Schema fields",
    )
