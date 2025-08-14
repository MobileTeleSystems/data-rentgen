# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.tag import Tag


class TagValue(Base):
    __tablename__ = "tag_value"
    __table_args__ = (UniqueConstraint("tag_id", "value"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tag_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("tag.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    tag: Mapped[Tag] = relationship(
        Tag,
        lazy="noload",
        foreign_keys=[tag_id],
    )
    value: Mapped[str] = mapped_column(String(256), nullable=False)
