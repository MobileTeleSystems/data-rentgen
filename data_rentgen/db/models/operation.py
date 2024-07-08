# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, DateTime, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType
from uuid6 import UUID

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.run import Run
from data_rentgen.db.models.status import Status


class Operation(Base):
    __tablename__ = "operation"
    __table_args__ = (
        PrimaryKeyConstraint("started_at", "id"),
        {"postgresql_partition_by": "RANGE (started_at)"},
    )

    id: Mapped[UUID] = mapped_column(SQL_UUID)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Start time of the operation",
    )

    run_id: Mapped[UUID] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Run operation is a part of",
    )
    run: Mapped[Run] = relationship(
        Run,
        primaryjoin="Operation.run_id == Run.id",
        lazy="noload",
        foreign_keys=[run_id],
    )

    status: Mapped[Status] = mapped_column(
        ChoiceType(Status),
        nullable=False,
        default=Status.STARTED,
        doc="Operation status info",
    )

    name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        doc="Name of the operation, e.g. Spark jobDescription",
    )

    type: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Type of the operation, e.g. BATCH, STREAMING",
    )

    description: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Operation description, e.g. documentation",
    )

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the operation",
    )
