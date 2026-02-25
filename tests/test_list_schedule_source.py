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
async def test_time_index_not_eagerly_cleaned_on_delete(redis_url: str) -> None:
    """Test that delete_schedule does NOT eagerly remove the index entry.
    This avoids a race condition where a concurrent add_schedule at the
    same minute could lose its index entry."""
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

    # Index entry is still present (lazy cleanup handles it later).
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1


@pytest.mark.anyio
async def test_cleanup_removes_old_empty_entries(redis_url: str) -> None:
    """Test that _cleanup_time_index removes index entries that are
    older than 1 hour and whose time key lists are empty."""
    prefix = uuid.uuid4().hex
    with freeze_time("2025-01-01 00:00:00"):
        source = ListRedisScheduleSource(redis_url, prefix=prefix)
        old_time = datetime.datetime(
            2024, 12, 31, 22, 0, tzinfo=datetime.timezone.utc,
        )
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=old_time,
        )
        await source.add_schedule(schedule)
        # Prevent delete_schedule from triggering cleanup by pretending
        # cleanup just ran (rate limiter blocks it).
        import time

        source._last_cleanup_time = time.monotonic()
        await source.delete_schedule(schedule.schedule_id)

    # Index still has the stale entry (cleanup was rate-limited).
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1

    # Run cleanup directly — entry is > 1 hour old and empty.
    with freeze_time("2025-01-01 00:00:00"):
        async with Redis(connection_pool=source._connection_pool) as redis:
            await source._cleanup_time_index(redis)

    # Now it should be cleaned up.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 0


@pytest.mark.anyio
async def test_cleanup_keeps_non_empty_entries(redis_url: str) -> None:
    """Test that _cleanup_time_index does NOT remove index entries whose
    time key lists still have schedules, even if older than 1 hour."""
    prefix = uuid.uuid4().hex
    with freeze_time("2025-01-01 00:00:00"):
        source = ListRedisScheduleSource(redis_url, prefix=prefix)
        old_time = datetime.datetime(
            2024, 12, 31, 22, 0, tzinfo=datetime.timezone.utc,
        )
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=old_time,
        )
        await source.add_schedule(schedule)

    # Run cleanup — entry is > 1 hour old but list is NOT empty.
    with freeze_time("2025-01-01 00:00:00"):
        async with Redis(connection_pool=source._connection_pool) as redis:
            await source._cleanup_time_index(redis)

    # Entry should still be present.
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1


@pytest.mark.anyio
async def test_cleanup_keeps_recent_empty_entries(redis_url: str) -> None:
    """Test that _cleanup_time_index does NOT remove index entries that
    are less than 1 hour old, even if their time key lists are empty."""
    prefix = uuid.uuid4().hex
    with freeze_time("2025-01-01 00:00:00"):
        source = ListRedisScheduleSource(redis_url, prefix=prefix)
        # 30 minutes ago — within the 1-hour safety window.
        recent_time = datetime.datetime(
            2024, 12, 31, 23, 30, tzinfo=datetime.timezone.utc,
        )
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=recent_time,
        )
        await source.add_schedule(schedule)
        await source.delete_schedule(schedule.schedule_id)

    # Run cleanup — entry is empty but only 30 min old.
    with freeze_time("2025-01-01 00:00:00"):
        async with Redis(connection_pool=source._connection_pool) as redis:
            await source._cleanup_time_index(redis)

    # Entry should still be present (not old enough).
    async with Redis(connection_pool=source._connection_pool) as redis:
        assert await redis.zcard(source._get_time_index_key()) == 1


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
async def test_post_send_triggers_cleanup(redis_url: str) -> None:
    """Test the full lifecycle: add schedule, get it, post_send it,
    then verify cleanup (triggered from delete_schedule) removes
    the stale index entry when it's > 1 hour old."""
    prefix = uuid.uuid4().hex

    with freeze_time("2025-01-01 02:00:00"):
        source = ListRedisScheduleSource(redis_url, prefix=prefix)
        schedule = ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            time=datetime.datetime(
                2025, 1, 1, 0, 30, tzinfo=datetime.timezone.utc,
            ),
        )
        await source.add_schedule(schedule)

        # First run picks up past schedules.
        schedules = await source.get_schedules()
        assert schedules == [schedule]

        # post_send -> delete_schedule -> _maybe_cleanup_time_index.
        # The entry is > 1 hour old and the list becomes empty,
        # so cleanup should remove it.
        for s in schedules:
            await source.post_send(s)

        async with Redis(connection_pool=source._connection_pool) as redis:
            assert await redis.zcard(source._get_time_index_key()) == 0

    # Second run should return nothing.
    with freeze_time("2025-01-01 02:01:00"):
        schedules = await source.get_schedules()
        assert schedules == []


@pytest.mark.anyio
async def test_cleanup_rate_limited(redis_url: str) -> None:
    """Test that _maybe_cleanup_time_index only runs once per minute."""
    prefix = uuid.uuid4().hex

    with freeze_time("2025-01-01 02:00:00"):
        source = ListRedisScheduleSource(redis_url, prefix=prefix)
        old_time = datetime.datetime(
            2025, 1, 1, 0, 30, tzinfo=datetime.timezone.utc,
        )
        sched1 = ScheduledTask(
            task_name="task1",
            labels={},
            args=[],
            kwargs={},
            time=old_time,
        )
        sched2 = ScheduledTask(
            task_name="task2",
            labels={},
            args=[],
            kwargs={},
            time=old_time,
        )
        await source.add_schedule(sched1)
        await source.add_schedule(sched2)

        # First delete triggers cleanup (first call, _last_cleanup_time=0).
        # But the time key list still has sched2, so the entry is kept.
        await source.delete_schedule(sched1.schedule_id)
        async with Redis(connection_pool=source._connection_pool) as redis:
            assert await redis.zcard(source._get_time_index_key()) == 1

        # Second delete happens within the same minute, so cleanup
        # is rate-limited and does NOT run — index entry remains
        # even though the list is now empty.
        await source.delete_schedule(sched2.schedule_id)
        async with Redis(connection_pool=source._connection_pool) as redis:
            assert await redis.zcard(source._get_time_index_key()) == 1


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
