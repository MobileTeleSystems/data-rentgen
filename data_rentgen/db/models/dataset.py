# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.storage import Storage


class Dataset(Base):
    __tablename__ = "dataset"
    __table_args__ = (UniqueConstraint("storage_id", "name"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    storage_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("storage.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Where dataset's data is actually stored (database, filesystem)",
    )
    storage: Mapped[Storage] = relationship(Storage, lazy="noload")

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

    alias_for_dataset_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
        doc="Where dataset's data is actually stored, e.g. LOCATION of a TABLE",
    )
    alias_for_dataset: Mapped[Dataset] = relationship("Dataset", lazy="noload")
