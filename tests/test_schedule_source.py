import uuid

import pytest
from taskiq import ScheduledTask

from taskiq_redis import RedisScheduleSource


@pytest.mark.anyio
async def test_set_schedule(redis_url: str) -> None:
    prefix = uuid.uuid4().hex
    source = RedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        "test_task",
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
        "test_task",
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
        "test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    await source.shutdown()
