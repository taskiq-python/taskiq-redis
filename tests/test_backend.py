import asyncio
import uuid
from typing import Any, TypeVar

import pytest
from taskiq import TaskiqResult

from taskiq_redis import RedisAsyncResultBackend
from taskiq_redis.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)

_ReturnType = TypeVar("_ReturnType")


class ResultForTest:
    """Just test class for testing."""

    def __init__(self) -> None:
        """Generates test class for result testing."""
        self.test_arg = uuid.uuid4()


@pytest.fixture
def task_id() -> str:
    """
    Generates ID for taskiq result.

    :returns: uuid as string.
    """
    return str(uuid.uuid4())


@pytest.fixture
def default_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates default TaskiqResult.

    :returns: TaskiqResult with generic result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value="Best test ever.",
        execution_time=0.1,
    )


@pytest.fixture
def custom_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates custom TaskiqResult.

    :returns: TaskiqResult with custom class result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value=ResultForTest(),
        execution_time=0.1,
    )


@pytest.mark.anyio
async def test_success_backend_default_result(
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests normal behavior with default result in TaskiqResult.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
    )
    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    result = await backend.get_result(task_id=task_id)

    assert result == default_taskiq_result
    await backend.shutdown()


@pytest.mark.anyio
async def test_success_backend_custom_result(
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests normal behavior with custom result in TaskiqResult.

    :param custom_taskiq_result: TaskiqResult with custom result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
    )
    await backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )
    result = await backend.get_result(task_id=task_id)

    assert (
        result.return_value.test_arg  # type: ignore
        == custom_taskiq_result.return_value.test_arg  # type: ignore
    )
    assert result.is_err == custom_taskiq_result.is_err
    assert result.execution_time == custom_taskiq_result.execution_time
    assert result.log == custom_taskiq_result.log
    await backend.shutdown()


@pytest.mark.anyio
async def test_cant_specify_ex_and_px_params(
    redis_url: str,
) -> None:
    """
    Tests the impossibility of specifying this and this at the same time.

    :param redis_url: url to redis.
    """
    with pytest.raises(DuplicateExpireTimeSelectedError):
        RedisAsyncResultBackend(redis_url, result_ex_time=1, result_px_time=1)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "ex_time, px_time",
    [(0, 0), (-500, 0), (0, -500), (-500, -500)],
)
async def test_ex_or_px_must_be_more_than_zero(
    ex_time: int,
    px_time: int,
    redis_url: str,
) -> None:
    """
    Tests that at least ex or px params must be specified.

    :param redis_url: url to redis.
    """
    with pytest.raises(ExpireTimeMustBeMoreThanZeroError):
        RedisAsyncResultBackend(
            redis_url,
            result_ex_time=ex_time,
            result_px_time=px_time,
        )


@pytest.mark.anyio
async def test_success_backend_expire_ex_param(
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests ex param.

    Here we test normal behavior, so we get result before expire time.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
        result_ex_time=1,
    )
    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await asyncio.sleep(0.5)

    result = await backend.get_result(task_id=task_id)

    assert result == default_taskiq_result
    await backend.shutdown()


@pytest.mark.anyio
async def test_unsuccess_backend_expire_ex_param(
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests ex param.

    Here we test bad behavior, so we can't get result
    because expire time is over.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
        result_ex_time=1,
    )
    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await asyncio.sleep(1.1)

    with pytest.raises(ResultIsMissingError):
        await backend.get_result(task_id=task_id)
    await backend.shutdown()


@pytest.mark.anyio
async def test_success_backend_expire_px_param(
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests px param.

    Here we test normal behavior, so we get result before expire time.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
        result_px_time=1000,
    )
    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await asyncio.sleep(0.5)

    result = await backend.get_result(task_id=task_id)

    assert result == default_taskiq_result
    await backend.shutdown()


@pytest.mark.anyio
async def test_unsuccess_backend_expire_px_param(
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
    redis_url: str,
) -> None:
    """
    Tests px param.

    Here we test bad behavior, so we can't get result
    because expire time is over.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    backend: RedisAsyncResultBackend[_ReturnType] = RedisAsyncResultBackend(
        redis_url,
        result_px_time=1000,
    )
    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await asyncio.sleep(1.1)

    with pytest.raises(ResultIsMissingError):
        await backend.get_result(task_id=task_id)
    await backend.shutdown()
