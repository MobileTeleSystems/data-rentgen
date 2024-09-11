import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.get_event_loop()
    yield loop
    loop.close()
