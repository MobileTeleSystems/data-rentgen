import time
from dataclasses import dataclass
from typing import Callable

import pytest
import pytest_asyncio

from data_rentgen.db.models import User
from data_rentgen.server.settings import ServerApplicationSettings
from data_rentgen.server.settings.auth.jwt import JWTSettings
from data_rentgen.server.utils.jwt import sign_jwt


@dataclass
class MockedUser:
    user: User
    access_token: str


@pytest_asyncio.fixture
async def mocked_user(
    user: User,
    access_token_factory: Callable[[int], str],
) -> MockedUser:
    access_token = access_token_factory(user.id)
    yield MockedUser(
        user=user.name,
        access_token=access_token,
    )


@pytest.fixture
def access_token_settings(server_app_settings: ServerApplicationSettings) -> JWTSettings:
    return JWTSettings.model_validate(server_app_settings.auth.access_token)


@pytest.fixture
def access_token_factory(access_token_settings: JWTSettings) -> Callable[[int], str]:
    def _generate_access_token(user_id: int) -> str:
        return sign_jwt(
            {"user_id": user_id, "exp": time.time() + 1000},
            access_token_settings.secret_key.get_secret_value(),
            access_token_settings.security_algorithm,
        )

    return _generate_access_token
