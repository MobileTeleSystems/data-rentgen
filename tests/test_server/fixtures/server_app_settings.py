# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import pytest

from data_rentgen.server.settings import ServerApplicationSettings


@pytest.fixture(scope="session", params=[{}])
def server_app_settings(request: pytest.FixtureRequest) -> ServerApplicationSettings:
    return ServerApplicationSettings.model_validate(request.param)
