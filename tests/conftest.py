import os

import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def redis_url() -> str:
    """
    URL to connect to redis.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: URL string.
    """
    return os.environ.get("TEST_REDIS_URL", "redis://localhost:7000")


@pytest.fixture
def redis_cluster_url() -> str:
    """
    URL to connect to redis cluster.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: URL string.
    """
    return os.environ.get("TEST_REDIS_CLUSTER_URL", "redis://localhost:7001")
