# SPDX-FileCopyrightText: 2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, String, text
from sqlalchemy.dialects.postgresql import TSTZRANGE, ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class CustomUserProperties(Base):
    __tablename__ = "custom_user_properties"
    __table_args__ = (
        ExcludeConstraint(  # noqa: WPS317
            ("user_id", "="),
            ("property_id", "="),
            ("during", "&&"),
            name="uq__custom_user_properties__user_id_property_id_during",
            using="gist",
        ),
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("user.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Id of corresponding user",
    )

    property_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("custom_properties.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Id of user's custom property",
    )

    value: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    during: Mapped[TSTZRANGE] = mapped_column(
        TSTZRANGE,
        server_defaults=text("tstzrange(now(), NULL, '[)')"),
        nullable=False,
    )
