# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
pytest_plugins = [
    "tests.fixtures.event_loop",
    "tests.fixtures.settings",
    "tests.fixtures.test_app",
    "tests.fixtures.test_client",
    "tests.fixtures.alembic",
    "tests.fixtures.async_engine",
    "tests.fixtures.async_session",
]
