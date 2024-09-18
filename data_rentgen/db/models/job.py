# SPDX-FileCopyrightText: 2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

from sqlalchemy import BigInteger, Computed, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.location import Location


class JobType(str, Enum):
    AIRFLOW_DAG = "AIRFLOW_DAG"
    AIRFLOW_TASK = "AIRFLOW_TASK"
    SPARK_APPLICATION = "SPARK_APPLICATION"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value


class Job(Base):
    __tablename__ = "job"
    __table_args__ = (
        UniqueConstraint("location_id", "name"),
        Index("ix__job__search_vector", "search_vector", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    location_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("location.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
        doc="Where job is located (Airflow instance, Spark cluster)",
    )
    location: Mapped[Location] = relationship(Location, lazy="noload")

    name: Mapped[str] = mapped_column(
        String,
        index=True,
        nullable=False,
        doc="Job name, e.g. Airflow DAG name + task name, or Spark applicationName",
    )

    type: Mapped[JobType] = mapped_column(
        ChoiceType(JobType, impl=String(32)),
        index=True,
        nullable=False,
        default=JobType.UNKNOWN,
        doc="Job type, e.g. AIRFLOW_DAG, AIRFLOW_TASK, SPARK_APPLICATION",
    )

    search_vector: Mapped[str] = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english'::regconfig, name || ' ' || (translate(name, '/.', '  ')))", persisted=True),
        nullable=False,
        deferred=True,
        doc="Full-text search vector",
    )
