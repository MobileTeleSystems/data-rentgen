# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, DateTime, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType  # type: ignore[import-untyped]
from uuid6 import UUID

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.operation import Operation
from data_rentgen.db.models.run import Run
from data_rentgen.db.models.schema import Schema


class OutputType(str, Enum):
    CREATE = "CREATE"
    ALTER = "ALTER"
    RENAME = "RENAME"

    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"

    DROP = "DROP"
    TRUNCATE = "TRUNCATE"

    def __str__(self) -> str:
        return self.value


# no foreign keys to avoid scanning all the partitions
class Output(Base):
    __tablename__ = "output"
    __table_args__ = (
        PrimaryKeyConstraint("created_at", "id"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Timestamp component of UUID, used for table partitioning",
    )
    id: Mapped[UUID] = mapped_column(SQL_UUID)

    operation_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Operation caused this output",
    )
    operation: Mapped[Operation] = relationship(
        Operation,
        primaryjoin="Output.operation_id == Operation.id",
        lazy="noload",
        foreign_keys=[operation_id],
    )

    run_id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=False,
        doc="Run caused this input operation",
    )
    run: Mapped[Run] = relationship(
        Run,
        primaryjoin="Output.run_id == Run.id",
        lazy="noload",
        foreign_keys=[run_id],
    )

    job_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Parent job of run",
    )
    job: Mapped[Job] = relationship(
        Job,
        primaryjoin="Output.job_id == Job.id",
        lazy="noload",
        foreign_keys=[job_id],
    )

    dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Dataset the output is performed against",
    )
    dataset: Mapped[Dataset] = relationship(
        Dataset,
        primaryjoin="Output.dataset_id == Dataset.id",
        lazy="noload",
        foreign_keys=[dataset_id],
    )

    type: Mapped[OutputType] = mapped_column(
        ChoiceType(OutputType, impl=String(32)),
        nullable=False,
        default=OutputType.APPEND,
        doc="Type of the output, e.g. READ, CREATE, APPEND",
    )

    schema_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Schema the output is performed with, if any",
    )
    schema: Mapped[Schema | None] = relationship(
        Schema,
        primaryjoin="Output.schema_id == Schema.id",
        lazy="noload",
        foreign_keys=[schema_id],
    )

    num_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during Output, in bytes",
    )

    num_rows: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during Output, in rows",
    )

    num_files: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="Amount of data moved during Output, in rows",
    )
