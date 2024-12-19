import base64
import logging

import pytest
import responses
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset, User
from data_rentgen.server.settings import ServerApplicationSettings as Settings
from data_rentgen.server.settings.auth.keycloak import KeycloakSettings
from tests.test_server.utils.enrich import enrich_datasets

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
    k_settings = KeycloakSettings.model_validate(server_app_settings.auth.keycloak)

    response = await test_client.get("/v1/datasets")

    # redirect unauthorized user to Keycloak
    state = str(response.url).encode("utf-8")
    state = base64.b64encode(state)
    redirect_url = (
        f"{k_settings.server_url}/realms/{k_settings.realm_name}/protocol/openid-connect/auth?client_id="
        f"{k_settings.client_id}&response_type=code&redirect_uri={k_settings.redirect_uri}"
        f"&scope={k_settings.scope}&state={state.decode('utf-8')}&nonce="
    )
    assert response.status_code == 401
    assert response.json() == {
        "error": {
            "code": "auth_redirect",
            "message": "Authorize on provided url",
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
    datasets: list[Dataset],
    server_app_settings: Settings,
    create_session_cookie,
    mock_keycloak_well_known,
    mock_keycloak_realm,
):
    datasets = await enrich_datasets(datasets, async_session)
    session_cookie = create_session_cookie(user)
    headers = {
        "Cookie": f"session={session_cookie}",
    }

    response = await test_client.get(
        "/v1/datasets",
        headers=headers,
    )

    assert response.cookies.get("session") == session_cookie
    assert response.status_code == 200
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(datasets),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.name)
        ],
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
async def test_get_keycloak_user_expired_access_token(
    caplog,
    user: User,
    datasets: list[Dataset],
    test_client: AsyncClient,
    async_session: AsyncSession,
    server_app_settings: Settings,
    create_session_cookie,
    mock_keycloak_well_known,
    mock_keycloak_realm,
    mock_keycloak_token_refresh,
):
    datasets = await enrich_datasets(datasets, async_session)
    session_cookie = create_session_cookie(user, expire_in_msec=-100000000)  # expired access token
    headers = {
        "Cookie": f"session={session_cookie}",
    }

    with caplog.at_level(logging.DEBUG):
        response = await test_client.get(
            "/v1/datasets",
            headers=headers,
        )

    assert response.cookies.get("session") != session_cookie, caplog.text  # cookie is updated
    assert response.status_code == 200
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(datasets),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "kind": "DATASET",
                "id": dataset.id,
                "format": dataset.format,
                "name": dataset.name,
                "location": {
                    "id": dataset.location.id,
                    "name": dataset.location.name,
                    "type": dataset.location.type,
                    "addresses": [{"url": address.url} for address in dataset.location.addresses],
                    "external_id": dataset.location.external_id,
                },
            }
            for dataset in sorted(datasets, key=lambda x: x.name)
        ],
    }
