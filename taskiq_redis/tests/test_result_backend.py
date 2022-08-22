import uuid

import pytest
from taskiq import TaskiqResult

from taskiq_redis.redis_backend import RedisAsyncResultBackend


@pytest.mark.anyio
async def test_set_result_success(redis_url: str) -> None:
    """
    Tests that results can be set without errors.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
    )
    task_id = uuid.uuid4().hex
    result: "TaskiqResult[int]" = TaskiqResult(
        is_err=True,
        log="My Log",
        return_value=11,
        execution_time=112.2,
    )
    await result_backend.set_result(
        task_id=task_id,
        result=result,
    )

    fetched_result = await result_backend.get_result(
        task_id=task_id,
        with_logs=True,
    )
    assert fetched_result.log == "My Log"
    assert fetched_result.return_value == 11
    assert fetched_result.execution_time == 112.2  # noqa: WPS459
    assert fetched_result.is_err


@pytest.mark.anyio
async def test_fetch_without_logs(redis_url: str) -> None:
    """
    Check if fetching value without logs works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
    )
    task_id = uuid.uuid4().hex
    result: "TaskiqResult[int]" = TaskiqResult(
        is_err=True,
        log="My Log",
        return_value=11,
        execution_time=112.2,
    )
    await result_backend.set_result(
        task_id=task_id,
        result=result,
    )

    fetched_result = await result_backend.get_result(
        task_id=task_id,
        with_logs=False,
    )
    assert fetched_result.log is None
    assert fetched_result.return_value == 11
    assert fetched_result.execution_time == 112.2  # noqa: WPS459
    assert fetched_result.is_err
