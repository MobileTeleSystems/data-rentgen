import time
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import User
from data_rentgen.server.settings.auth.jwt import JWTSettings
from data_rentgen.server.utils.jwt import sign_jwt
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_dummy_auth_invalid_token(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token + 'invalid'}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Invalid token"},
    }


async def test_dummy_auth_expired_token(
    test_client: AsyncClient,
    user: User,
    access_token_settings: JWTSettings,
):
    token = sign_jwt(
        {"user_id": user.id, "exp": time.time() - 10000},
        access_token_settings.secret_key.get_secret_value(),
        access_token_settings.security_algorithm,
    )

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {"code": "unauthorized", "details": None, "message": "Invalid token"},
    }


async def test_dummy_auth_unexisting_user(
    test_client: AsyncClient,
    user: User,
    access_token_settings: JWTSettings,
):
    token = sign_jwt(
        {"user_id": user.id + 666, "exp": time.time() + 100000},
        access_token_settings.secret_key.get_secret_value(),
        access_token_settings.security_algorithm,
    )

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
    assert response.json() == {
        "error": {
            "code": "not_found",
            "details": {"entity_type": "User", "field": "user_id", "value": user.id + 666},
            "message": f"User with user_id={user.id + 666} not found",
        },
    }


async def test_dummy_auth_logout_not_implemented(
    test_client: AsyncClient,
    user: User,
    access_token_settings: JWTSettings,
):
    token = sign_jwt(
        {"user_id": user.id, "exp": time.time() + 100000},
        access_token_settings.secret_key.get_secret_value(),
        access_token_settings.security_algorithm,
    )

    response = await test_client.get(
        "v1/auth/logout",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.NOT_IMPLEMENTED, response.json()
    assert response.json() == {
        "error": {
            "code": "not_implemented",
            "details": None,
            "message": "Logout method is not implemented for DummyAuthProvider.",
        },
    }


async def test_dummy_auth_generate_valid_token(
    test_client: AsyncClient,
):
    response = await test_client.post("v1/auth/token", data={"username": "test", "password": "test"})

    token = response.json()["access_token"]

    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
