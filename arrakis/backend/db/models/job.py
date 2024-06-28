# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from arrakis.backend.db.models.base import Base
from arrakis.backend.db.models.location import Location


class Job(Base):
    __tablename__ = "job"
    __table_args__ = (UniqueConstraint("location_id", "name"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Where dataset's data is actually stored (database, filesystem)",
    )
    location: Mapped[Location] = relationship(Location, lazy="selectin")

    name: Mapped[str] = mapped_column(
        String(255),
        index=True,
        doc="Job name, e.g. Airflow DAG name + task name, or Spark applicationName",
    )
