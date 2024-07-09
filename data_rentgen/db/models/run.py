# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, DateTime, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType
from uuid6 import UUID

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.job import Job
from data_rentgen.db.models.runner import Runner
from data_rentgen.db.models.status import Status
from data_rentgen.db.models.user import User


# no foreign keys to avoid scanning all the partitions
class Run(Base):
    __tablename__ = "run"
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
    parent: Mapped[Run | None] = relationship(
        "Run",
        primaryjoin="Run.parent_run_id == Run.id",
        lazy="noload",
        foreign_keys=[parent_run_id],
    )

    status: Mapped[Status] = mapped_column(
        ChoiceType(Status),
        nullable=False,
        default=Status.STARTED,
        doc="Run status info",
    )

    runner_id: Mapped[int] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
        doc="Runner the run is running on, e.g. Airflow, Yarn",
    )
    runner: Mapped[Runner | None] = relationship(
        Runner,
        primaryjoin="Run.runner_id == Runner.id",
        lazy="noload",
        foreign_keys=[runner_id],
    )

    name: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        doc="Name of the run, e.g. Spark applicationId",
    )
    attempt: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        doc="Attempt number of the run",
    )
    description: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Run description, e.g. Airflow task docstring",
    )
    log_url: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="Log url of the run, if any",
    )

    started_at: Mapped[datetime] = mapped_column(
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

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="End time of the run",
    )
    ended_reason: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
        doc="End reason of the run, e.g. exception string",
    )
