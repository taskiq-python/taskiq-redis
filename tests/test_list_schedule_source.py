import datetime
import uuid
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from taskiq import ScheduledTask

from taskiq_redis.list_schedule_source import ListRedisScheduleSource
from taskiq_redis.schedule_source import RedisScheduleSource


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_cron(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
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
async def test_schedule_interval(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=datetime.timedelta(seconds=5),
    )
    await source.add_schedule(schedule)
    scehdules = await source.get_schedules()
    assert scehdules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_schedule_from_past(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
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
    schedules = await source.get_schedules()
    assert schedules == [schedule]
    # Past schedules are returned on every call until they are sent.
    assert await source.get_schedules() == [schedule]
    for schedule in schedules:
        await source.post_send(schedule)
    # Sent schedules are removed from the source.
    schedules = await source.get_schedules()
    assert schedules == []

    # Schedule that lands in an already passed minute after the first run
    # is picked up on the first call of the next minute.
    late_schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=2),
    )
    await source.add_schedule(late_schedule)
    with freeze_time("2025-01-01 00:01:00"):
        assert await source.get_schedules() == [late_schedule]


@pytest.mark.anyio
async def test_past_schedules_lookup_once_per_minute(redis_url: str) -> None:
    """Test that past time keys are scanned once per minute, not on every call."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    with (
        patch.object(
            source,
            "_get_previous_time_schedules",
            wraps=source._get_previous_time_schedules,
        ) as lookup,
        freeze_time("2025-01-01 00:00:00") as frozen,
    ):
        for _ in range(60):
            await source.get_schedules()
            frozen.tick()
        assert lookup.await_count == 1
        await source.get_schedules()
        assert lookup.await_count == 2


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_removal_time(redis_url: str) -> None:
    """Test adding a cron schedule."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
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
async def test_removal_cron(redis_url: str) -> None:
    """Test removing cron schedules."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
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
async def test_removal_interval(redis_url: str) -> None:
    """Test removing cron schedules."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=datetime.timedelta(seconds=30),
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

    source = ListRedisScheduleSource(
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
