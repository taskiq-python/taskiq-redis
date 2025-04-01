import datetime
import uuid

import pytest
from freezegun import freeze_time
from taskiq import ScheduledTask

from taskiq_redis.array_schedule_source import ArrayRedisScheduleSource
from taskiq_redis.schedule_source import RedisScheduleSource


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_cron(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ArrayRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_from_past(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ArrayRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]
    # After getting the schedules for the second time,
    # all the schedules in the past are ignored.
    scehdules = await source.get_schedules()
    assert scehdules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_removal(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ArrayRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=4),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == []
    # Assert that we will get the schedule after the time has passed.
    with freeze_time("2025-01-01 00:04:00"):
        scehdules = await source.get_schedules()
        assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_deletion(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ArrayRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc),
    )
    await source.add_schedule(schedule)
    # When running for the first time, the scheduler will get all the
    # schedules that are in the past.
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]
    await source.delete_schedule(schedule.schedule_id)
    scehdules = await source.get_schedules()
    assert scehdules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_migration(redis_url: str) -> None:
    """Test adding a cron schedule."""
    new_prefix = uuid.uuid4().hex
    old_prefix = uuid.uuid4().hex
    old_source = RedisScheduleSource(redis_url, prefix=old_prefix)

    for i in range(30):
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=i),
        )
        await old_source.add_schedule(schedule)

    old_schedules = await old_source.get_schedules()

    source = ArrayRedisScheduleSource(
        redis_url,
        prefix=new_prefix,
        skip_past_schedules=True,
    ).with_migrate_from(
        old_source,
        delete_schedules=True,
    )

    await source.startup()
    assert await old_source.get_schedules() == []

    for old_schedule in old_schedules:
        with freeze_time(old_schedule.time):
            assert await source.get_schedules() == [old_schedule]
