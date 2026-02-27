import datetime
import time as _time
from logging import getLogger
from typing import Any

from redis.asyncio import BlockingConnectionPool, Redis
from taskiq import ScheduledTask, ScheduleSource
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer
from typing_extensions import Self

logger = getLogger("taskiq.redis_schedule_source")


class ListRedisScheduleSource(ScheduleSource):
    """Schedule source based on arrays."""

    def __init__(
        self,
        url: str,
        prefix: str = "schedule",
        max_connection_pool_size: int | None = None,
        serializer: TaskiqSerializer | None = None,
        buffer_size: int = 50,
        skip_past_schedules: bool = False,
        populate_time_index: bool = False,
        **connection_kwargs: Any,
    ) -> None:
        """
        Create a new schedule source.

        :param url: Redis URL
        :param prefix: Prefix for all the keys
        :param max_connection_pool_size: Maximum size of the connection pool
        :param serializer: Serializer to use for the schedules
        :param buffer_size: Buffer size for getting schedules
        :param skip_past_schedules: Skip schedules that are in the past.
        :param populate_time_index: If True, on startup run a one-time SCAN
            to populate the time index sorted set from existing time keys.
            This is needed for migrating from an older version that did not
            maintain the time index. Set this to True once to backfill the
            index, then set it back to False for subsequent runs.
        :param connection_kwargs: Additional connection kwargs
        """
        super().__init__()
        self._prefix = prefix
        self._buffer_size = buffer_size
        self._connection_pool = BlockingConnectionPool.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        if serializer is None:
            serializer = PickleSerializer()
        self._serializer = serializer
        self._previous_schedule_source: ScheduleSource | None = None
        self._delete_schedules_after_migration: bool = True
        self._skip_past_schedules = skip_past_schedules
        self._populate_time_index = populate_time_index
        self._last_cleanup_time: float = 0

    async def startup(self) -> None:
        """
        Startup the schedule source.

        By default this function does nothing.
        But if the previous schedule source is set,
        it will try to migrate schedules from it.

        If populate_time_index is True, it will scan for existing
        time keys and populate the time index sorted set.
        """
        if self._previous_schedule_source is not None:
            logger.info("Migrating schedules from previous source")
            await self._previous_schedule_source.startup()
            schedules = await self._previous_schedule_source.get_schedules()
            logger.info(f"Found {len(schedules)}")
            for schedule in schedules:
                await self.add_schedule(schedule)
                if self._delete_schedules_after_migration:
                    await self._previous_schedule_source.delete_schedule(
                        schedule.schedule_id,
                    )
            await self._previous_schedule_source.shutdown()
            logger.info("Migration complete")

        if self._populate_time_index:
            logger.info("Populating time index from existing keys via scan")
            async with Redis(connection_pool=self._connection_pool) as redis:
                batch: dict[str, float] = {}
                async for key in redis.scan_iter(f"{self._prefix}:time:*"):
                    key_str = key.decode()
                    key_time = self._parse_time_key(key_str)
                    if key_time:
                        batch[key_str] = key_time.timestamp()
                        if len(batch) >= self._buffer_size:
                            await redis.zadd(
                                self._get_time_index_key(),
                                batch,
                            )
                            batch = {}
                if batch:
                    await redis.zadd(self._get_time_index_key(), batch)
            logger.info("Time index population complete")

    def _get_time_key(self, time: datetime.datetime) -> str:
        """Get the key for a time-based schedule."""
        if time.tzinfo is None:
            time = time.replace(tzinfo=datetime.timezone.utc)
        iso_time = time.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M")
        return f"{self._prefix}:time:{iso_time}"

    def _get_time_index_key(self) -> str:
        """Get the key for the time index sorted set."""
        return f"{self._prefix}:time_index"

    def _get_cron_key(self) -> str:
        """Get the key for a cron-based schedule."""
        return f"{self._prefix}:cron"

    def _get_interval_key(self) -> str:
        return f"{self._prefix}:interval"

    def _get_data_key(self, schedule_id: str) -> str:
        """Get the key for a schedule data."""
        return f"{self._prefix}:data:{schedule_id}"

    def _parse_time_key(self, key: str) -> datetime.datetime | None:
        """Get time value from the timed-key."""
        try:
            dt_str = key.split(":", 2)[2]
            return datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M").replace(
                tzinfo=datetime.timezone.utc,
            )
        except ValueError:
            logger.debug("Failed to parse time key %s", key)
            return None

    async def _maybe_cleanup_time_index(self, redis: Redis) -> None:  # type: ignore[type-arg]
        """
        Run time index cleanup at most once per minute.

        Called from delete_schedule after removing a time-based schedule,
        since that's the path where time key lists become empty.
        """
        now = _time.monotonic()
        if now - self._last_cleanup_time < 60:
            return
        self._last_cleanup_time = now
        await self._cleanup_time_index(redis)

    async def _cleanup_time_index(self, redis: Redis) -> None:  # type: ignore[type-arg]
        """
        Remove stale entries from the time index sorted set.

        Only removes entries that are older than 5 minutes AND whose
        corresponding time key list is empty (or no longer exists).
        This avoids a race condition where an eager cleanup in
        delete_schedule could remove an index entry right as
        add_schedule is creating a new schedule at the same minute.
        """
        five_minutes_ago = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(minutes=5)
        ).timestamp()
        stale_keys: list[bytes] = await redis.zrangebyscore(
            self._get_time_index_key(),
            "-inf",
            five_minutes_ago,
        )
        for key in stale_keys:
            if await redis.llen(key) == 0:
                await redis.zrem(self._get_time_index_key(), key)

    async def _get_previous_time_schedules(
        self,
        current_time: datetime.datetime,
    ) -> list[bytes]:
        """
        Function that gets all timed schedules that are in the past.

        Since this source doesn't retrieve all the schedules at once,
        we need to get all the schedules that are in the past and haven't
        been sent yet.

        Uses the time index sorted set to look up past time keys
        instead of scanning all Redis keys.

        Called on every get_schedules invocation so that schedules
        added in a past minute (after the previous get_schedules call
        but before the minute rolled over) are never missed.

        :param current_time: The reference time captured by the caller,
            used to derive the cutoff so that the "previous" and "current"
            windows never overlap.
        """
        logger.info("Getting previous time schedules")
        minute_before = current_time.replace(
            second=0, microsecond=0,
        ) - datetime.timedelta(
            minutes=1,
        )
        schedules = []
        async with Redis(connection_pool=self._connection_pool) as redis:
            max_score = minute_before.timestamp()
            time_keys: list[bytes] = await redis.zrangebyscore(
                self._get_time_index_key(),
                "-inf",
                max_score,
            )
            for key in time_keys:
                schedules.extend(await redis.lrange(key, 0, -1))  # type: ignore[misc]

        return schedules

    async def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule from the source."""
        async with Redis(connection_pool=self._connection_pool) as redis:
            schedule = await redis.getdel(self._get_data_key(schedule_id))
            if schedule is not None:
                logger.debug("Deleting schedule %s", schedule_id)
                schedule = model_validate(
                    ScheduledTask,
                    self._serializer.loadb(schedule),
                )
                # We need to remove the schedule from the cron or time list.
                if schedule.cron is not None:
                    await redis.lrem(self._get_cron_key(), 0, schedule_id)  # type: ignore[misc]
                elif schedule.time is not None:
                    time_key = self._get_time_key(schedule.time)
                    await redis.lrem(time_key, 0, schedule_id)  # type: ignore[misc]
                    await self._maybe_cleanup_time_index(redis)
                elif schedule.interval:
                    await redis.lrem(self._get_interval_key(), 0, schedule_id)  # type: ignore[misc]

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """Add a schedule to the source."""
        async with Redis(connection_pool=self._connection_pool) as redis:
            # At first we set data key which contains the schedule data.
            await redis.set(
                f"{self._prefix}:data:{schedule.schedule_id}",
                self._serializer.dumpb(model_dump(schedule)),
            )
            # Then we add the schedule to the cron or time list.
            # This is an optimization, so we can get all the schedules
            # for the current time much faster.
            if schedule.cron is not None:
                await redis.rpush(self._get_cron_key(), schedule.schedule_id)  # type: ignore[misc]
            elif schedule.time is not None:
                time_key = self._get_time_key(schedule.time)
                await redis.rpush(time_key, schedule.schedule_id)  # type: ignore[misc]
                # Add to the time index sorted set so we can look up
                # past time keys without scanning all Redis keys.
                time_val = schedule.time
                if time_val.tzinfo is None:
                    time_val = time_val.replace(tzinfo=datetime.timezone.utc)
                score = (
                    time_val.astimezone(datetime.timezone.utc)
                    .replace(second=0, microsecond=0)
                    .timestamp()
                )
                await redis.zadd(  # type: ignore[misc]
                    self._get_time_index_key(),
                    {time_key: score},
                )
            elif schedule.interval:
                await redis.rpush(  # type: ignore[misc]
                    self._get_interval_key(),
                    schedule.schedule_id,
                )

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)

    async def get_schedules(self) -> list["ScheduledTask"]:
        """
        Get all schedules.

        This function gets all the schedules from the schedule source.
        What it does is get all the cron schedules, interval schedules,
        past time schedules, and current-minute time schedules and
        return them.

        Past time schedules are fetched on every call so that
        schedules added after the previous call but before the
        minute rolled over are never missed.
        """
        schedules = []
        current_time = datetime.datetime.now(datetime.timezone.utc)
        timed: list[bytes] = []
        if not self._skip_past_schedules:
            timed = await self._get_previous_time_schedules(current_time)
        async with Redis(connection_pool=self._connection_pool) as redis:
            buffer = []
            crons = await redis.lrange(self._get_cron_key(), 0, -1)  # type: ignore[misc]
            logger.debug("Got %d cron schedules", len(crons))
            if crons:
                buffer.extend(crons)
            intervals = await redis.lrange(self._get_interval_key(), 0, -1)  # type: ignore[misc]
            logger.debug("Got %d interval schedules", len(intervals))
            if intervals:
                buffer.extend(intervals)
            timed.extend(await redis.lrange(self._get_time_key(current_time), 0, -1))  # type: ignore[misc]
            logger.debug("Got %d timed schedules", len(timed))
            if timed:
                buffer.extend(timed)
            while buffer:
                schedules.extend(
                    await redis.mget(
                        (
                            self._get_data_key(x.decode())
                            for x in buffer[: self._buffer_size]
                        ),
                    ),
                )
                buffer = buffer[self._buffer_size :]

        return [
            model_validate(ScheduledTask, self._serializer.loadb(schedule))
            for schedule in schedules
            if schedule
        ]

    def with_migrate_from(
        self,
        source: ScheduleSource,
        delete_schedules: bool = True,
    ) -> Self:
        """
        Enable migration from previous schedule source.

        If this function is called during declaration,
        the source will try to migrate schedules from the previous source.

        :param source: previous schedule source
        :param delete_schedules: delete schedules during migration process
            from the previous source.
        """
        self._previous_schedule_source = source
        self._delete_schedules_after_migration = delete_schedules
        return self
