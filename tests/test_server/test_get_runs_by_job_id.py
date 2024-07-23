from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Run

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_job_id_empty(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/runs/by_job_id")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_get_runs_by_job_id_missing(
    test_client: AsyncClient,
    new_run: Run,
):
    response = await test_client.get(
        "v1/runs/by_job_id",
        params={"since": new_run.created_at.isoformat(), "job_id": new_run.job_id},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 0,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [],
    }


async def test_get_run_by_job_id(
    test_client: AsyncClient,
    run_with_all_fields: Run,
    async_session: AsyncSession,
):
    query = select(Run).where(Run.id == run_with_all_fields.id).options(selectinload(Run.started_by_user))
    run_from_db: Run = await async_session.scalar(query)

    response = await test_client.get(
        "v1/runs/by_job_id",
        params={"since": run_with_all_fields.created_at.isoformat(), "job_id": run_with_all_fields.job_id},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 1,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(run_from_db.id),
                "job_id": run_from_db.job_id,
                "parent_run_id": str(run_from_db.parent_run_id),
                "status": run_from_db.status.value,
                "external_id": run_from_db.external_id,
                "attempt": run_from_db.attempt,
                "persistent_log_url": run_from_db.persistent_log_url,
                "running_log_url": run_from_db.running_log_url,
                "started_at": run_from_db.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run_from_db.started_by_user.name},
                "start_reason": run_from_db.start_reason,
                "ended_at": run_from_db.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run_from_db.end_reason,
            },
        ],
    }


async def test_get_runs_by_job_id(
    test_client: AsyncClient,
    runs_with_same_job: list[Run],
    async_session: AsyncSession,
):
    query = (
        select(Run)
        .where(Run.id.in_([run.id for run in runs_with_same_job]))
        .order_by(Run.id)
        .options(selectinload(Run.started_by_user))
    )
    scalars = await async_session.scalars(query)
    runs_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/runs/by_job_id",
        params={"job_id": runs_with_same_job[0].job_id, "since": runs_with_same_job[0].created_at.isoformat()},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 5,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(run.id),
                "job_id": run.job_id,
                "parent_run_id": run.parent_run_id,
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at,
                "started_by_user": run.started_by_user,
                "start_reason": run.start_reason,
                "ended_at": run.ended_at,
                "end_reason": run.end_reason,
            }
            for run in runs_from_db
        ],
    }


async def test_get_runs_time_range(
    test_client: AsyncClient,
    runs_with_same_job: list[Run],
    async_session: AsyncSession,
):
    query = (
        select(Run)
        .where(Run.id.in_([run.id for run in runs_with_same_job]))
        .order_by(Run.id)
        .options(selectinload(Run.started_by_user))
    )
    scalars = await async_session.scalars(query)
    runs_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/runs/by_job_id",
        params={
            "job_id": runs_with_same_job[0].job_id,
            "since": runs_with_same_job[0].created_at.isoformat(),
            "until": (runs_with_same_job[0].created_at + timedelta(seconds=1)).isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert len(runs_from_db) == 5
    assert response.json()["meta"]["total_count"] == 2
