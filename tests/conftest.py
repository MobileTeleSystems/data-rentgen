# SPDX-FileCopyrightText: 2023 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
pytest_plugins = [
    "tests.fixtures.event_loop",
    "tests.fixtures.db_settings",
    "tests.fixtures.async_engine",
    "tests.fixtures.async_session",
    "tests.test_database.fixtures.alembic",
    "tests.test_consumer.fixtures.consumer_app_settings",
    "tests.test_consumer.fixtures.test_broker",
    "tests.test_server.fixtures.server_app_settings",
    "tests.test_server.fixtures.test_server_app",
    "tests.test_server.fixtures.test_client",
]
