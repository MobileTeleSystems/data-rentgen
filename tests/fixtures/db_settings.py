import pytest

from data_rentgen.db.settings import DatabaseSettings


@pytest.fixture(scope="session", params=[{}])
def db_settings(request: pytest.FixtureRequest) -> DatabaseSettings:
    return DatabaseSettings.model_validate(request.param)
