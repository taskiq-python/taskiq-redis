import datetime
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
        self._is_first_run = True
        self._previous_schedule_source: ScheduleSource | None = None
        self._delete_schedules_after_migration: bool = True
        self._skip_past_schedules = skip_past_schedules

    async def startup(self) -> None:
        """
        Startup the schedule source.

        By default this function does nothing.
        But if the previous schedule source is set,
        it will try to migrate schedules from it.
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

    def _get_time_key(self, time: datetime.datetime) -> str:
        """Get the key for a time-based schedule."""
        if time.tzinfo is None:
            time = time.replace(tzinfo=datetime.timezone.utc)
        iso_time = time.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M")
        return f"{self._prefix}:time:{iso_time}"

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

    async def _get_previous_time_schedules(self) -> list[bytes]:
        """
        Function that gets all timed schedules that are in the past.

        Since this source doesn't retrieve all the schedules at once,
        we need to get all the schedules that are in the past and haven't
        been sent yet.

        We do this by getting all the time keys and checking if the time
        is less than the current time.

        This function is called only during the first run to minimize
        the number of requests to the Redis server.
        """
        logger.info("Getting previous time schedules")
        minute_before = datetime.datetime.now(
            datetime.timezone.utc,
        ).replace(second=0, microsecond=0) - datetime.timedelta(
            minutes=1,
        )
        schedules = []
        async with Redis(connection_pool=self._connection_pool) as redis:
            time_keys: list[str] = []
            # We need to get all the time keys and check if the time is less than
            # the current time.
            async for key in redis.scan_iter(f"{self._prefix}:time:*"):
                key_time = self._parse_time_key(key.decode())
                if key_time and key_time <= minute_before:
                    time_keys.append(key.decode())
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
                await redis.rpush(  # type: ignore[misc]
                    self._get_time_key(schedule.time),
                    schedule.schedule_id,
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
        What it does is get all the cron schedules and time schedules
        for the current time and return them.

        If it's the first run, it also gets all the time schedules
        that are in the past and haven't been sent yet.
        """
        schedules = []
        current_time = datetime.datetime.now(datetime.timezone.utc)
        timed: list[bytes] = []
        # Only during first run, we need to get previous time schedules
        if not self._skip_past_schedules:
            timed = await self._get_previous_time_schedules()
            self._is_first_run = False
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
