# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class Storage(Base):
    __tablename__ = "storage"
    __table_args__ = (Index(None, "location_id", "namespace", unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    location: Mapped[Location] = relationship(Location, lazy="noload")

    # bucket or database name
    namespace: Mapped[str | None] = mapped_column(String(255), nullable=True)
