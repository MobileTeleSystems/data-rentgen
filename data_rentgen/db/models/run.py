# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import (
    BigInteger,
    Computed,
    DateTime,
    Index,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType
from uuid6 import UUID

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.status import Status
from data_rentgen.db.models.user import User


class RunStartReason(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"

    def __str__(self) -> str:
        return str(self.value)


# no foreign keys to avoid scanning all the partitions
class Run(Base):
    __tablename__ = "run"
    __table_args__ = (
        PrimaryKeyConstraint("created_at", "id"),
        Index("ix_run_search_vector", "search_vector", postgresql_using="gin"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Timestamp component of UUID, used for table partitioning",
    )
    id: Mapped[UUID] = mapped_column(SQL_UUID)

    job_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=False,
        doc="Job the run is associated with",
    )
    job: Mapped[Job] = relationship(
        Job,
        primaryjoin="Run.job_id == Job.id",
        lazy="noload",
        foreign_keys=[job_id],
    )

    parent_run_id: Mapped[UUID | None] = mapped_column(
        SQL_UUID,
        index=True,
        nullable=True,
        doc="Parent of current run, e.g. Airflow task run which started Spark application",
    )
    parent_run: Mapped[Run | None] = relationship(
        "Run",
        primaryjoin="Run.parent_run_id == Run.id",
        lazy="noload",
        foreign_keys=[parent_run_id],
    )

    status: Mapped[Status] = mapped_column(
        ChoiceType(Status, impl=String(32)),
        nullable=False,
        default=Status.UNKNOWN,
        doc="Run status info",
    )

    external_id: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="External ID of the run, e.g. Spark applicationId",
    )
    attempt: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Attempt number of the run",
    )
    persistent_log_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Persistent log url of the run, like Spark history server url, optional",
    )
    running_log_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Log url of the run in progress, like Spark session UI url, optional",
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Start time of the run",
    )
    started_by_user_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        doc="User who started the run",
    )
    started_by_user: Mapped[User | None] = relationship(
        "User",
        primaryjoin="Run.started_by_user_id == User.id",
        lazy="noload",
        foreign_keys=[started_by_user_id],
    )
    start_reason: Mapped[RunStartReason | None] = mapped_column(
        ChoiceType(RunStartReason, impl=String(32)),
        nullable=True,
        doc="Start reason of the run, e.g. MANUAL or AUTOMATIC",
    )

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the run",
    )
    end_reason: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="End reason of the run, e.g. exception string",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english'::regconfig, COALESCE(name, ''::text))", persisted=True),
        nullable=False,
        doc="Full-text search vector",
    )
