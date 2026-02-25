import datetime
import uuid

import pytest
from freezegun import freeze_time
from redis.asyncio import BlockingConnectionPool, Redis
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
    for schedule in schedules:
        await source.post_send(schedule)
    # After getting the schedules for the second time,
    # all the schedules in the past are ignored.
    schedules = await source.get_schedules()
    assert schedules == []


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


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_time_index_populated_on_add(redis_url: str) -> None:
    """Test that adding a time schedule populates the time index sorted set."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=5),
    )
    await source.add_schedule(schedule)

    # Verify the time index sorted set has an entry.
    async with Redis(connection_pool=source._connection_pool) as redis:
        members = await redis.zrange(source._get_time_index_key(), 0, -1)
        assert len(members) == 1
        assert members[0].decode() == source._get_time_key(schedule.time)


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_time_index_cleaned_on_delete(redis_url: str) -> None:
    """Test that deleting last schedule from a time key cleans the index."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=5),
    )
    await source.add_schedule(schedule)

    # Index has 1 entry.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1

    await source.delete_schedule(schedule.schedule_id)

    # After deletion, the index should be empty.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 0
        # The time key list itself should also be deleted.
        assert not await redis.exists(source._get_time_key(schedule.time))


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_time_index_not_cleaned_when_other_schedules_remain(
    redis_url: str,
) -> None:
    """Test that deleting one schedule doesn't remove the index entry
    when other schedules still exist at the same time."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule_time = datetime.datetime.now(
        datetime.timezone.utc,
    ) + datetime.timedelta(minutes=5)
    schedule1 = ScheduledTask(
        task_name="test_task_1",
        labels={},
        args=[],
        kwargs={},
        time=schedule_time,
    )
    schedule2 = ScheduledTask(
        task_name="test_task_2",
        labels={},
        args=[],
        kwargs={},
        time=schedule_time,
    )
    await source.add_schedule(schedule1)
    await source.add_schedule(schedule2)

    await source.delete_schedule(schedule1.schedule_id)

    # Index should still have the entry because schedule2 remains.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1

    await source.delete_schedule(schedule2.schedule_id)

    # Now the index should be empty.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 0


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_past_schedules_found_via_time_index(redis_url: str) -> None:
    """Test that past schedules are discovered via the time index
    instead of a full SCAN."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    past_time = datetime.datetime.now(
        datetime.timezone.utc,
    ) - datetime.timedelta(minutes=5)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=past_time,
    )
    await source.add_schedule(schedule)

    # First call to get_schedules should find the past schedule via time index.
    schedules = await source.get_schedules()
    assert schedules == [schedule]


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_populate_time_index_from_existing_keys(redis_url: str) -> None:
    """Test that populate_time_index=True backfills the sorted set
    from existing time keys created without the index."""
    prefix = uuid.uuid4().hex

    # Simulate old-style data: create time key lists directly in Redis
    # without populating the time index sorted set.
    pool = BlockingConnectionPool.from_url(url=redis_url)
    past_times = [
        datetime.datetime(2024, 12, 31, 23, 55, tzinfo=datetime.timezone.utc),
        datetime.datetime(2024, 12, 31, 23, 56, tzinfo=datetime.timezone.utc),
        datetime.datetime(2024, 12, 31, 23, 57, tzinfo=datetime.timezone.utc),
    ]

    source_for_keys = ListRedisScheduleSource(redis_url, prefix=prefix)
    async with Redis(connection_pool=pool) as redis:
        for t in past_times:
            time_key = source_for_keys._get_time_key(t)
            # Push a dummy schedule ID directly (bypassing add_schedule
            # to simulate old behavior without time index).
            await redis.rpush(time_key, f"sched_{t.minute}")  # type: ignore[misc]

        # Verify no time index exists yet.
        assert await redis.zcard(source_for_keys._get_time_index_key()) == 0
    await pool.disconnect()

    # Now create a source with populate_time_index=True.
    source = ListRedisScheduleSource(
        redis_url,
        prefix=prefix,
        populate_time_index=True,
    )
    await source.startup()

    # The time index should now be populated.
    async with Redis(connection_pool=source._connection_pool) as redis:
        count = await redis.zcard(source._get_time_index_key())
        assert count == len(past_times)


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_post_send_cleans_time_index(redis_url: str) -> None:
    """Test that post_send (which calls delete_schedule for time tasks)
    properly cleans up the time index."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    schedule = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=3),
    )
    await source.add_schedule(schedule)

    # First run picks up past schedules.
    schedules = await source.get_schedules()
    assert schedules == [schedule]

    # Simulate sending the task.
    for s in schedules:
        await source.post_send(s)

    # Time index should be empty now.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 0

    # Second run should return nothing.
    schedules = await source.get_schedules()
    assert schedules == []


@pytest.mark.anyio
@freeze_time("2025-01-01 00:00:00")
async def test_cron_and_interval_not_in_time_index(redis_url: str) -> None:
    """Test that cron and interval schedules do not affect the time index."""
    prefix = uuid.uuid4().hex
    source = ListRedisScheduleSource(redis_url, prefix=prefix)
    cron_schedule = ScheduledTask(
        task_name="cron_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    interval_schedule = ScheduledTask(
        task_name="interval_task",
        labels={},
        args=[],
        kwargs={},
        interval=datetime.timedelta(seconds=30),
    )
    await source.add_schedule(cron_schedule)
    await source.add_schedule(interval_schedule)

    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 0
