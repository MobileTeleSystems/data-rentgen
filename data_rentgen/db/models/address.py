# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, Computed, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class Address(Base):
    __tablename__ = "address"
    __table_args__ = (
        UniqueConstraint("location_id", "url"),
        Index("ix__address__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Location the address is associated with",
    )

    location: Mapped[Location] = relationship(
        Location,
        lazy="noload",
        back_populates="addresses",
    )

    url: Mapped[str] = mapped_column(
        String,
        index=True,
        nullable=False,
        doc="Address in URL format",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english'::regconfig, url || ' ' || (translate(url, '/.', '  ')))", persisted=True),
        nullable=False,
        doc="Full-text search vector",
    )
