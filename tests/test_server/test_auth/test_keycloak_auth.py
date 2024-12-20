import logging

import pytest
import responses
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset, User
from data_rentgen.server.settings import ServerApplicationSettings as Settings
from data_rentgen.server.settings.auth.keycloak import KeycloakSettings

KEYCLOAK_PROVIDER = "data_rentgen.server.providers.auth.keycloak_provider.KeycloakAuthProvider"
pytestmark = [pytest.mark.asyncio, pytest.mark.server]


@responses.activate
@pytest.mark.parametrize(
    "server_app_settings",
    [
        {
            "auth": {
                "provider": KEYCLOAK_PROVIDER,
            },
        },
    ],
    indirect=True,
)
async def test_get_keycloak_user_unauthorized(
    test_client: AsyncClient,
    mock_keycloak_well_known,
    caplog,
    server_app_settings: Settings,
):
    settings = KeycloakSettings.model_validate(server_app_settings.auth.keycloak)

    response = await test_client.get("/v1/users/me")

    # redirect unauthorized user to Keycloak
    redirect_url = (
        f"{settings.server_url}/realms/{settings.realm_name}/protocol/openid-connect/auth?client_id="
        f"{settings.client_id}&response_type=code&redirect_uri={settings.redirect_uri}"
        f"&scope={settings.scope}&state=&nonce="
    )
    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "auth_redirect",
            "message": "Please authorize using provided URL",
            "details": redirect_url,
        },
    }


@responses.activate
@pytest.mark.parametrize(
    "server_app_settings",
    [
        {
            "auth": {
                "provider": KEYCLOAK_PROVIDER,
            },
        },
    ],
    indirect=True,
)
async def test_get_keycloak_user_authorized(
    test_client: AsyncClient,
    async_session: AsyncSession,
    user: User,
    server_app_settings: Settings,
    create_session_cookie,
    mock_keycloak_well_known,
    mock_keycloak_realm,
):
    session_cookie = create_session_cookie(user)
    headers = {
        "Cookie": f"session={session_cookie}",
    }

    response = await test_client.get(
        "/v1/users/me",
        headers=headers,
    )

    assert response.cookies.get("session") == session_cookie
    assert response.status_code == 200
    assert response.json() == {"name": user.name}


@responses.activate
@pytest.mark.parametrize(
    "server_app_settings",
    [
        {
            "auth": {
                "provider": KEYCLOAK_PROVIDER,
            },
        },
    ],
    indirect=True,
)
async def test_get_keycloak_user_expired_access_token(
    caplog,
    user: User,
    test_client: AsyncClient,
    async_session: AsyncSession,
    server_app_settings: Settings,
    create_session_cookie,
    mock_keycloak_well_known,
    mock_keycloak_realm,
    mock_keycloak_token_refresh,
):
    session_cookie = create_session_cookie(user, expire_in_msec=-100000000)  # expired access token
    headers = {
        "Cookie": f"session={session_cookie}",
    }

    with caplog.at_level(logging.DEBUG):
        response = await test_client.get(
            "/v1/users/me",
            headers=headers,
        )

    assert response.cookies.get("session") != session_cookie, caplog.text  # cookie is updated
    assert response.status_code == 200
    assert response.json() == {"name": user.name}
