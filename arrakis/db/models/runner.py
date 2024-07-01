# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from arrakis.db.models.base import Base


class Runner(Base):
    __tablename__ = "runner"
    __table_args__ = (UniqueConstraint("location_id", "type", "version"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Runner location",
    )
    type: Mapped[str] = mapped_column(
        String(64),
        index=True,
        doc="Runner type, e.g. spark, airflow, flink, trino",
    )
    version: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Runner version",
    )
