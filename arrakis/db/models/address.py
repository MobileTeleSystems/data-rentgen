# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from arrakis.db.models.base import Base
from arrakis.db.models.location import Location


class Address(Base):
    __tablename__ = "address"
    __table_args__ = (UniqueConstraint("location_id", "url"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Location the address is associated with",
    )

    location: Mapped[list[Location]] = relationship(Location, lazy="selectin", back_populates="addresses")

    url: Mapped[str] = mapped_column(
        String(255),
        index=True,
        doc="Address in URL format",
    )