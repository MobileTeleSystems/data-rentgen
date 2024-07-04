# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import pytest

from data_rentgen.consumer.settings import ConsumerApplicationSettings


@pytest.fixture(scope="session", params=[{}])
def consumer_app_settings(request: pytest.FixtureRequest) -> ConsumerApplicationSettings:
    return ConsumerApplicationSettings.model_validate(request.param)
