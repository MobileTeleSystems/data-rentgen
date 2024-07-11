# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class Dataset(Base):
    __tablename__ = "dataset"
    __table_args__ = (UniqueConstraint("location_id", "name"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Where dataset's data is actually located (database address, filesystem address)",
    )
    location: Mapped[Location] = relationship(
        Location,
        lazy="noload",
        foreign_keys=[location_id],
    )

    name: Mapped[str] = mapped_column(
        String(255),
        index=True,
        nullable=False,
        doc="Dataset name, e.g. table name or filesystem path",
    )
    format: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Data format, if any",
    )
