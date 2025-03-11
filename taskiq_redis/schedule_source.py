import sys
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, List, Optional, Tuple

from redis.asyncio import (
    BlockingConnectionPool,
    Connection,
    Redis,
    RedisCluster,
    Sentinel,
)
from taskiq import ScheduleSource
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.serializers import PickleSerializer

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    _Redis: TypeAlias = Redis[bytes]  # type: ignore
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore
else:
    _Redis: TypeAlias = Redis
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool


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
        self.connection_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
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
        self.redis: "RedisCluster" = RedisCluster.from_url(
            url,
            **connection_kwargs,
        )
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        await self.redis.delete(f"{self.prefix}:{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to redis.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        await self.redis.set(
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
        async for key in self.redis.scan_iter(f"{self.prefix}:*"):
            raw_schedule = await self.redis.get(key)
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

    async def shutdown(self) -> None:
        """Shut down the schedule source."""
        await self.redis.aclose()


class RedisSentinelScheduleSource(ScheduleSource):
    """
    Source of schedules for redis cluster.

    This class allows you to store schedules in redis.
    Also it supports dynamic schedules.

    :param sentinels: list of sentinel host and ports pairs.
    :param master_name: sentinel master name.
    :param prefix: prefix for redis schedule keys.
    :param buffer_size: buffer size for redis scan.
        This is how many keys will be fetched at once.
    :param max_connection_pool_size: maximum number of connections in pool.
    :param serializer: serializer for data.
    :param connection_kwargs: additional arguments for RedisCluster.
    """

    def __init__(
        self,
        sentinels: List[Tuple[str, int]],
        master_name: str,
        prefix: str = "schedule",
        buffer_size: int = 50,
        serializer: Optional[TaskiqSerializer] = None,
        min_other_sentinels: int = 0,
        sentinel_kwargs: Optional[Any] = None,
        **connection_kwargs: Any,
    ) -> None:
        self.prefix = prefix
        self.sentinel = Sentinel(
            sentinels=sentinels,
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )
        self.master_name = master_name
        self.buffer_size = buffer_size
        if serializer is None:
            serializer = PickleSerializer()
        self.serializer = serializer

    @asynccontextmanager
    async def _acquire_master_conn(self) -> AsyncIterator[_Redis]:
        async with self.sentinel.master_for(self.master_name) as redis_conn:
            yield redis_conn

    async def delete_schedule(self, schedule_id: str) -> None:
        """Remove schedule by id."""
        async with self._acquire_master_conn() as redis:
            await redis.delete(f"{self.prefix}:{schedule_id}")

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        """
        Add schedule to redis.

        :param schedule: schedule to add.
        :param schedule_id: schedule id.
        """
        async with self._acquire_master_conn() as redis:
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
        async with self._acquire_master_conn() as redis:
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
        for sentinel in self.sentinel.sentinels:
            await sentinel.aclose()
