# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from arrakis.db.models.base import Base


class Location(Base):
    """Some network location where data is bound to"""

    __tablename__ = "location"
    __table_args__ = (Index(None, "type", "name", unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    type: Mapped[str] = mapped_column(
        String(64),
        doc="Location type, e.g. kafka, postgres, hdfs",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        doc="Location name, e.g. cluster name",
    )
