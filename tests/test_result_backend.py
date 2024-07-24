import asyncio
import uuid
from typing import List, Tuple

import pytest
from taskiq import TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress, TaskState

from taskiq_redis import (
    RedisAsyncClusterResultBackend,
    RedisAsyncResultBackend,
    RedisAsyncSentinelResultBackend,
)
from taskiq_redis.exceptions import ResultIsMissingError


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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_remove_results_after_reading(redis_url: str) -> None:
    """
    Check if removing results after reading works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
        keep_results=False,
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

    await result_backend.get_result(task_id=task_id)
    with pytest.raises(ResultIsMissingError):
        await result_backend.get_result(task_id=task_id)

    await result_backend.shutdown()


@pytest.mark.anyio
async def test_keep_results_after_reading(redis_url: str) -> None:
    """
    Check if keeping results after reading works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
        keep_results=True,
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

    res1 = await result_backend.get_result(task_id=task_id)
    res2 = await result_backend.get_result(task_id=task_id)
    assert res1 == res2
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_result_max_connections(redis_url: str) -> None:
    """
    Tests that asynchronous backend works with connection limit.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
        max_connection_pool_size=1,
        timeout=3,
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

    async def get_result() -> None:
        await result_backend.get_result(task_id=task_id, with_logs=True)

    await asyncio.gather(*[get_result() for _ in range(10)])
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_result_success_cluster(redis_cluster_url: str) -> None:
    """
    Tests that results can be set without errors in cluster mode.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncClusterResultBackend(  # type: ignore
        redis_url=redis_cluster_url,
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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_fetch_without_logs_cluster(redis_cluster_url: str) -> None:
    """
    Check if fetching value without logs works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncClusterResultBackend(  # type: ignore
        redis_url=redis_cluster_url,
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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_remove_results_after_reading_cluster(redis_cluster_url: str) -> None:
    """
    Check if removing results after reading works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncClusterResultBackend(  # type: ignore
        redis_url=redis_cluster_url,
        keep_results=False,
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

    await result_backend.get_result(task_id=task_id)
    with pytest.raises(ResultIsMissingError):
        await result_backend.get_result(task_id=task_id)

    await result_backend.shutdown()


@pytest.mark.anyio
async def test_keep_results_after_reading_cluster(redis_cluster_url: str) -> None:
    """
    Check if keeping results after reading works fine.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncClusterResultBackend(  # type: ignore
        redis_url=redis_cluster_url,
        keep_results=True,
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

    res1 = await result_backend.get_result(task_id=task_id)
    res2 = await result_backend.get_result(task_id=task_id)
    assert res1 == res2
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_result_success_sentinel(
    redis_sentinels: List[Tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Tests that results can be set without errors in cluster mode.

    :param redis_sentinels: list of host and port pairs.
    :param redis_sentinel_master_name: redis sentinel master name string.
    """
    result_backend = RedisAsyncSentinelResultBackend(  # type: ignore
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_fetch_without_logs_sentinel(
    redis_sentinels: List[Tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Check if fetching value without logs works fine.

    :param redis_sentinels: list of host and port pairs.
    :param redis_sentinel_master_name: redis sentinel master name string.
    """
    result_backend = RedisAsyncSentinelResultBackend(  # type: ignore
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
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
    assert fetched_result.execution_time == 112.2
    assert fetched_result.is_err
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_remove_results_after_reading_sentinel(
    redis_sentinels: List[Tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Check if removing results after reading works fine.

    :param redis_sentinels: list of host and port pairs.
    :param redis_sentinel_master_name: redis sentinel master name string.
    """
    result_backend = RedisAsyncSentinelResultBackend(  # type: ignore
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        keep_results=False,
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

    await result_backend.get_result(task_id=task_id)
    with pytest.raises(ResultIsMissingError):
        await result_backend.get_result(task_id=task_id)

    await result_backend.shutdown()


@pytest.mark.anyio
async def test_keep_results_after_reading_sentinel(
    redis_sentinels: List[Tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Check if keeping results after reading works fine.

    :param redis_sentinels: list of host and port pairs.
    :param redis_sentinel_master_name: redis sentinel master name string.
    """
    result_backend = RedisAsyncSentinelResultBackend(  # type: ignore
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        keep_results=True,
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

    res1 = await result_backend.get_result(task_id=task_id)
    res2 = await result_backend.get_result(task_id=task_id)
    assert res1 == res2
    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_progress(redis_url: str) -> None:
    """
    Test that set_progress/get_progress works.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncResultBackend(  # type: ignore
        redis_url=redis_url,
    )
    task_id = uuid.uuid4().hex

    test_progress_1 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "quarter way", "pct": 25},
    )
    test_progress_2 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "half way", "pct": 50},
    )

    # Progress starts as None
    assert await result_backend.get_progress(task_id=task_id) is None

    # Setting the first time persists
    await result_backend.set_progress(task_id=task_id, progress=test_progress_1)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_1

    # Setting the second time replaces the first
    await result_backend.set_progress(task_id=task_id, progress=test_progress_2)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_2

    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_progress_cluster(redis_cluster_url: str) -> None:
    """
    Test that set_progress/get_progress works in cluster mode.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncClusterResultBackend(  # type: ignore
        redis_url=redis_cluster_url,
    )
    task_id = uuid.uuid4().hex

    test_progress_1 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "quarter way", "pct": 25},
    )
    test_progress_2 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "half way", "pct": 50},
    )

    # Progress starts as None
    assert await result_backend.get_progress(task_id=task_id) is None

    # Setting the first time persists
    await result_backend.set_progress(task_id=task_id, progress=test_progress_1)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_1

    # Setting the second time replaces the first
    await result_backend.set_progress(task_id=task_id, progress=test_progress_2)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_2

    await result_backend.shutdown()


@pytest.mark.anyio
async def test_set_progress_sentinel(
    redis_sentinels: List[Tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Test that set_progress/get_progress works in cluster mode.

    :param redis_url: redis URL.
    """
    result_backend = RedisAsyncSentinelResultBackend(  # type: ignore
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
    )
    task_id = uuid.uuid4().hex

    test_progress_1 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "quarter way", "pct": 25},
    )
    test_progress_2 = TaskProgress(
        state=TaskState.STARTED,
        meta={"message": "half way", "pct": 50},
    )

    # Progress starts as None
    assert await result_backend.get_progress(task_id=task_id) is None

    # Setting the first time persists
    await result_backend.set_progress(task_id=task_id, progress=test_progress_1)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_1

    # Setting the second time replaces the first
    await result_backend.set_progress(task_id=task_id, progress=test_progress_2)

    fetched_result = await result_backend.get_progress(task_id=task_id)
    assert fetched_result == test_progress_2

    await result_backend.shutdown()
