from typing import Any, List, Optional

from redis.asyncio import BlockingConnectionPool, ConnectionPool, Redis, RedisCluster
from taskiq import ScheduleSource
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.scheduler.scheduled_task import ScheduledTask

from taskiq_redis.serializer import PickleSerializer


class RedisScheduleSource(ScheduleSource):
    """
    Source of schedules for redis.

    This class allows you to store schedules in redis.
    Also it supports dynamic schedules.

    :param url: url to redis.
    :param prefix: prefix for redis schedule keys.
    :param buffer_size: buffer size for redis scan.
        This is how many keys will be fetched at once.
    :param max_connection_pool_size: maximum number of connections in pool.
    :param serializer: serializer for data.
    :param connection_kwargs: additional arguments for redis BlockingConnectionPool.
    """

    def __init__(
        self,
        url: str,
        prefix: str = "schedule",
        buffer_size: int = 50,
        max_connection_pool_size: Optional[int] = None,
        serializer: Optional[TaskiqSerializer] = None,
        **connection_kwargs: Any,
    ) -> None:
        self.prefix = prefix
        self.connection_pool: ConnectionPool = BlockingConnectionPool.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.buffer_size = buffer_size
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            await redis.delete(f"{self.prefix}:{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to redis.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        async with Redis(connection_pool=self.connection_pool) as redis:
            await redis.set(
                f"{self.prefix}:{schedule.schedule_id}",
                self.serializer.dumpb(model_dump(schedule)),
            )

    async def get_schedules(self) -> List[ScheduledTask]:
        """
        Get all schedules from redis.

        This method is used by scheduler to get all schedules.

        :return: list of schedules.
        """
        schedules = []
        async with Redis(connection_pool=self.connection_pool) as redis:
            buffer = []
            async for key in redis.scan_iter(f"{self.prefix}:*"):
                buffer.append(key)
                if len(buffer) >= self.buffer_size:
                    schedules.extend(await redis.mget(buffer))
                    buffer = []
            if buffer:
                schedules.extend(await redis.mget(buffer))
        return [
            model_validate(ScheduledTask, self.serializer.loadb(schedule))
            for schedule in schedules
            if schedule
        ]

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)

    async def shutdown(self) -> None:
        """Shut down the schedule source."""
        await self.connection_pool.disconnect()


class RedisClusterScheduleSource(ScheduleSource):
    """
    Source of schedules for redis cluster.

    This class allows you to store schedules in redis.
    Also it supports dynamic schedules.

    :param url: url to redis cluster.
    :param prefix: prefix for redis schedule keys.
    :param buffer_size: buffer size for redis scan.
        This is how many keys will be fetched at once.
    :param max_connection_pool_size: maximum number of connections in pool.
    :param serializer: serializer for data.
    :param connection_kwargs: additional arguments for RedisCluster.
    """

    def __init__(
        self,
        url: str,
        prefix: str = "schedule",
        serializer: Optional[TaskiqSerializer] = None,
        **connection_kwargs: Any,
    ) -> None:
        self.prefix = prefix
        self.redis: RedisCluster[bytes] = RedisCluster.from_url(
            url,
            **connection_kwargs,
        )
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        await self.redis.delete(f"{self.prefix}:{schedule_id}")  # type: ignore[attr-defined]

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to redis.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        await self.redis.set(  # type: ignore[attr-defined]
            f"{self.prefix}:{schedule.schedule_id}",
            self.serializer.dumpb(model_dump(schedule)),
        )

    async def get_schedules(self) -> List[ScheduledTask]:
        """
        Get all schedules from redis.

        This method is used by scheduler to get all schedules.

        :return: list of schedules.
        """
        schedules = []
        async for key in self.redis.scan_iter(f"{self.prefix}:*"):  # type: ignore[attr-defined]
            raw_schedule = await self.redis.get(key)  # type: ignore[attr-defined]
            parsed_schedule = model_validate(
                ScheduledTask,
                self.serializer.loadb(raw_schedule),
            )
            schedules.append(parsed_schedule)
        return schedules

    async def post_send(self, task: ScheduledTask) -> None:
        """Delete a task after it's completed."""
        if task.time is not None:
            await self.delete_schedule(task.schedule_id)
