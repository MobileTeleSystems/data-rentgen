# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import pytest

from arrakis.db.settings import DatabaseSettings


@pytest.fixture(scope="session", params=[{}])
def db_settings(request: pytest.FixtureRequest) -> DatabaseSettings:
    return DatabaseSettings.model_validate(request.param)
