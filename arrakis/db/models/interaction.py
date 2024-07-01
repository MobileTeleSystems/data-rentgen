# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, DateTime, PrimaryKeyConstraint
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType
from uuid6 import UUID

from arrakis.db.models.base import Base
from arrakis.db.models.dataset import Dataset
from arrakis.db.models.operation import Operation
from arrakis.db.models.schema import Schema
from arrakis.db.models.user import User
from arrakis.db.utils.uuid import generate_new_uuid


def _default_uuid(context: DefaultExecutionContext):
    started_at: datetime = context.get_current_parameters()["started_at"]
    return generate_new_uuid(started_at)


class InteractionType(str, Enum):  # noqa: WPS600
    READ = "READ"

    CREATE = "CREATE"
    ALTER = "ALTER"
    RENAME = "RENAME"

    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"

    DROP = "DROP"
    TRUNCATE = "TRUNCATE"


# no foreign keys to avoid scanning all the partitions
class Interaction(Base):
    __tablename__ = "interaction"
    __table_args__ = (
        PrimaryKeyConstraint("started_at", "id"),
        {"postgresql_partition_by": "RANGE (started_at)"},
    )

    id: Mapped[UUID] = mapped_column(SQL_UUID, default=_default_uuid)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Start time of the interaction. Used only for partitioning",
    )

    operation_id: Mapped[UUID] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Operation caused this interaction",
    )
    operation: Mapped[Operation] = relationship(
        Operation,
        lazy="selectin",
        foreign_keys=[operation_id],
    )

    dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Dataset the interaction is performed against",
    )
    dataset: Mapped[Dataset] = relationship(Dataset, lazy="selectin", foreign_keys=[dataset_id])

    type: Mapped[InteractionType] = mapped_column(
        ChoiceType(InteractionType),
        nullable=False,
        default=InteractionType.APPEND,
        doc="Type of the interaction, e.g. READ, CREATE, APPEND",
    )

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the interaction",
    )

    schema_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Schema the interaction is performed with, if any",
    )
    schema: Mapped[Schema | None] = relationship(Schema, lazy="selectin", foreign_keys=[schema_id])

    connect_as_user_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Username used for dataset access",
    )
    as_user: Mapped[User | None] = relationship(User, lazy="selectin", foreign_keys=[connect_as_user_id])

    num_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during interaction, in bytes",
    )

    num_rows: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during interaction, in rows",
    )

    num_files: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during interaction, in rows",
    )
