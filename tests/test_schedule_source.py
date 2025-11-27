import asyncio
import datetime as dt
import uuid

import pytest
from taskiq import ScheduledTask

from taskiq_redis import (
    RedisClusterScheduleSource,
    RedisScheduleSource,
    RedisSentinelScheduleSource,
)


@pytest.mark.anyio
async def test_set_schedule(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_delete_schedule(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    schedules = await source.get_schedules()
    # Schedules are empty.
    assert not schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_post_run_cron(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_post_run_time(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=dt.datetime(2000, 1, 1),
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == []
    await source.shutdown()


@pytest.mark.anyio
async def test_buffer(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix, buffer_size=1)
    schedule1 = ScheduledTask(
        task_name="test_task1",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    schedule2 = ScheduledTask(
        task_name="test_task2",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule1)
    await source.add_schedule(schedule2)
    schedules = await source.get_schedules()
    assert len(schedules) == 2
    assert schedule1 in schedules
    assert schedule2 in schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_max_connections(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(
        redis_url,
        prefix=prefix,
        max_connection_pool_size=1,
        timeout=3,
    )
    await asyncio.gather(*[source.get_schedules() for _ in range(10)])


@pytest.mark.anyio
async def test_cluster_set_schedule(redis_cluster_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_cluster_delete_schedule(redis_cluster_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    schedules = await source.get_schedules()
    # Schedules are empty.
    assert not schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_cluster_post_run_cron(redis_cluster_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_cluster_post_run_time(redis_cluster_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=dt.datetime(2000, 1, 1),
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == []
    await source.shutdown()


@pytest.mark.anyio
async def test_cluster_get_schedules(redis_cluster_url: str) -> None:
    """
    Test of a redis cluster source.

    This test checks that if the schedules are located on different nodes,
    the source will still be able to get them all.

    To simulate this we set a specific shard key for each schedule.
    The shard keys are from this gist:

    https://gist.githubusercontent.com/dvirsky/93f43277317f629bb06e858946416f7e/raw/b0438faf6f5a0020c12a0730f6cd6ac4bdc4b171/crc16_slottable.h

    """
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix)
    schedule1 = ScheduledTask(
        schedule_id=r"id-{06S}",
        task_name="test_task1",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    schedule2 = ScheduledTask(
        schedule_id=r"id-{4Rs}",
        task_name="test_task2",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule1)
    await source.add_schedule(schedule2)
    schedules = await source.get_schedules()
    assert len(schedules) == 2
    assert schedule1 in schedules
    assert schedule2 in schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_sentinel_set_schedule(
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisSentinelScheduleSource(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        prefix=prefix,
    )
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_sentinel_delete_schedule(
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisSentinelScheduleSource(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        prefix=prefix,
    )
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    schedules = await source.get_schedules()
    # Schedules are empty.
    assert not schedules
    await source.shutdown()


@pytest.mark.anyio
async def test_sentinel_post_run_cron(
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisSentinelScheduleSource(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        prefix=prefix,
    )
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == [schedule]
    await source.shutdown()


@pytest.mark.anyio
async def test_sentinel_post_run_time(
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisSentinelScheduleSource(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        prefix=prefix,
    )
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=dt.datetime(2000, 1, 1),
    )
    await source.add_schedule(schedule)
    assert await source.get_schedules() == [schedule]
    await source.post_send(schedule)
    assert await source.get_schedules() == []
    await source.shutdown()


@pytest.mark.anyio
async def test_sentinel_buffer(
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    prefix = uuid.uuid4().hex
    source = RedisSentinelScheduleSource(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        prefix=prefix,
        buffer_size=1,
    )
    schedule1 = ScheduledTask(
        task_name="test_task1",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    schedule2 = ScheduledTask(
        task_name="test_task2",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule1)
    await source.add_schedule(schedule2)
    schedules = await source.get_schedules()
    assert len(schedules) == 2
    assert schedule1 in schedules
    assert schedule2 in schedules
    await source.shutdown()
