# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime, timedelta

from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
)
from data_rentgen.utils.uuid import generate_new_uuid


def generate_airflow_run(dag_id: str, task_id: str, created_at: datetime, ended_at: datetime) -> RunDTO:
    address = "https://airflow01.companyname.com"
    location = LocationDTO(
        type="https",
        name="airflow01.companyname.com",
        addresses={address},
    )

    dag_job = JobDTO(
        name=dag_id,
        location=location,
        type=JobTypeDTO(type="AIRFLOW_DAG"),
    )
    dag_run_id = f"scheduled__{created_at.isoformat()}"
    dag_ui = address + f"/dags/{dag_job.name}/runs/{dag_run_id}"
    dag_run = RunDTO(
        id=generate_new_uuid(created_at),
        job=dag_job,
        started_at=created_at,
        ended_at=ended_at,
        external_id=dag_run_id,
        persistent_log_url=dag_ui,
        status=RunStatusDTO.SUCCEEDED,
        start_reason=RunStartReasonDTO.AUTOMATIC,
    )

    task_job = JobDTO(
        name=f"{dag_id}.{task_id}",
        location=location,
        type=JobTypeDTO(type="AIRFLOW_TASK"),
    )
    return RunDTO(
        id=generate_new_uuid(created_at),
        job=task_job,
        parent_run=dag_run,
        started_at=created_at + timedelta(seconds=1),
        ended_at=ended_at - timedelta(seconds=1),
        external_id=dag_run_id,
        attempt="1",
        persistent_log_url=dag_ui + f"/tasks/{task_job.name}?try_number=1",
        status=RunStatusDTO.SUCCEEDED,
        start_reason=RunStartReasonDTO.AUTOMATIC,
    )
