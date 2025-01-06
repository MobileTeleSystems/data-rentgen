# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from sqlalchemy import BigInteger, Computed, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class Dataset(Base):
    __tablename__ = "dataset"
    __table_args__ = (
        UniqueConstraint("location_id", "name"),
        Index("ix__dataset__search_vector", "search_vector", postgresql_using="gin"),
    )

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
        String,
        index=True,
        nullable=False,
        doc="Dataset name, e.g. table name or filesystem path",
    )
    format: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        doc="Data format, if any",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed(
            # Postgres treated values like `mydb.mytable` as a whole word (domain name),
            # which does not allow user to search by name parts like `mytable`.
            # Same for slashes which are treated like file paths.
            # Keep both original name and one without punctuation to allow both full match and partial match.
            #
            # Also 'english' dictionary performs stemming,
            # so name like 'my.database.table' is converted to tsvector `'databas':2 'tabl':3`,
            # which does not match a tsquery like 'my:* & database:* & table:*'.
            # Instead prefer 'simple' dictionary as it does not use stemming.
            "to_tsvector('simple'::regconfig, name || ' ' || (translate(name, '/.', '  ')))",
            persisted=True,
        ),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
