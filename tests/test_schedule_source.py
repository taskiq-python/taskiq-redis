import datetime as dt
import uuid

import pytest
from taskiq import ScheduledTask

from taskiq_redis import RedisClusterScheduleSource, RedisScheduleSource


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
async def test_cluster_buffer(redis_cluster_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisClusterScheduleSource(redis_cluster_url, prefix=prefix, buffer_size=1)
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
