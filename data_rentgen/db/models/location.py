# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base

if TYPE_CHECKING:
    from data_rentgen.db.models.address import Address


class Location(Base):
    """Some network location where data is bound to"""

    __tablename__ = "location"
    __table_args__ = (Index(None, "type", "name", unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        doc="Location type, e.g. kafka, postgres, hdfs",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        doc="Location name, e.g. cluster name",
    )

    addresses: Mapped[list[Address]] = relationship(
        "Address",
        lazy="noload",
        back_populates="location",
    )
