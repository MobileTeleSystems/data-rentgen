from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from data_rentgen.db.models import Run

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_job_id_missing_fields(
    test_client: AsyncClient,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs",
        params={"since": since.isoformat()},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'job_id' and 'since', or 'run_id' field",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "run_id": [],
                        "job_id": None,
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_job_id_conflicting_fields(
    test_client: AsyncClient,
    new_run: Run,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "job_id": new_run.job_id,
            "run_id": str(new_run.id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": [],
                    "code": "value_error",
                    "message": "Value error, fields 'job_id','since', 'until' cannot be used if 'run_id' is set",
                    "context": {},
                    "input": {
                        "page": 1,
                        "page_size": 20,
                        "run_id": [str(new_run.id)],
                        "job_id": new_run.job_id,
                        "since": since.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "until": None,
                    },
                },
            ],
        },
    }


async def test_get_runs_by_job_id_until_less_than_since(
    test_client: AsyncClient,
    new_run: Run,
):
    since = new_run.created_at
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/runs",
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "job_id": str(new_run.job_id),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
            ],
        },
    }


async def test_get_runs_by_job_id_missing(
    test_client: AsyncClient,
    new_run: Run,
):
    response = await test_client.get(
        "v1/runs",
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
    run: Run,
    async_session: AsyncSession,
):
    query = select(Run).where(Run.id == run.id).options(selectinload(Run.started_by_user))
    run_from_db: Run = await async_session.scalar(query)

    response = await test_client.get(
        "v1/runs",
        params={
            "since": run.created_at.isoformat(),
            "job_id": run.job_id,
        },
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
                "kind": "RUN",
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
        "v1/runs",
        params={
            "since": runs_with_same_job[0].created_at.isoformat(),
            "job_id": runs_with_same_job[0].job_id,
        },
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
                "kind": "RUN",
                "id": str(run.id),
                "job_id": run.job_id,
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    since = runs_with_same_job[0].created_at
    until = since + timedelta(seconds=1)

    query = (
        select(Run)
        .where(Run.id.in_([run.id for run in runs_with_same_job]))
        .where(Run.created_at >= since)
        .where(Run.created_at <= until)
        .order_by(Run.id)
        .options(selectinload(Run.started_by_user))
    )
    scalars = await async_session.scalars(query)
    runs_from_db = list(scalars.all())

    response = await test_client.get(
        "v1/runs",
        params={
            "job_id": runs_with_same_job[0].job_id,
            "since": since.isoformat(),
            "until": until.isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 2,
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
                "parent_run_id": str(run.parent_run_id),
                "status": run.status.value,
                "external_id": run.external_id,
                "attempt": run.attempt,
                "persistent_log_url": run.persistent_log_url,
                "running_log_url": run.running_log_url,
                "started_at": run.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "started_by_user": {"name": run.started_by_user.name},
                "start_reason": run.start_reason,
                "ended_at": run.ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_reason": run.end_reason,
            }
            for run in runs_from_db
        ],
    }
