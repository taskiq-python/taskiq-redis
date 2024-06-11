import os
from typing import List, Tuple

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


@pytest.fixture
def redis_sentinels() -> List[Tuple[str, int]]:
    """
    List of redis sentinel hosts.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: list of host and port pairs.
    """
    sentinels = os.environ.get("TEST_REDIS_SENTINELS", "localhost:7002")
    host, _, port = sentinels.partition(":")
    return [(host, int(port))]


@pytest.fixture
def redis_sentinel_master_name() -> str:
    """
    Redis sentinel master name.

    It tries to get it from environ,
    and return default one if the variable is
    not set.

    :return: redis sentinel master name string.
    """
    return os.environ.get("TEST_REDIS_SENTINEL_MASTER_NAME", "mymaster")
