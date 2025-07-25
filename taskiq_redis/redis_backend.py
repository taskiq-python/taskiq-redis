import sys
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from redis.asyncio import BlockingConnectionPool, Redis, Sentinel
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import Connection
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult
from taskiq.serializers import PickleSerializer

from taskiq_redis.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)

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

_ReturnType = TypeVar("_ReturnType")

PROGRESS_KEY_SUFFIX = "__progress"


class RedisAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result based on redis."""

    def __init__(
        self,
        redis_url: str,
        keep_results: bool = True,
        result_ex_time: Optional[int] = None,
        result_px_time: Optional[int] = None,
        max_connection_pool_size: Optional[int] = None,
        serializer: Optional[TaskiqSerializer] = None,
        prefix_str: Optional[str] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new result backend.

        :param redis_url: url to redis.
        :param keep_results: flag to not remove results from Redis after reading.
        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        :param max_connection_pool_size: maximum number of connections in pool.
        :param connection_kwargs: additional arguments for redis BlockingConnectionPool.

        :raises DuplicateExpireTimeSelectedError: if result_ex_time
            and result_px_time are selected.
        :raises ExpireTimeMustBeMoreThanZeroError: if result_ex_time
            and result_px_time are equal zero.
        """
        self.redis_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
            url=redis_url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.prefix_str = prefix_str

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError

    def _task_name(self, task_id: str) -> str:
        if self.prefix_str is None:
            return task_id
        return f"{self.prefix_str}:{task_id}"

    async def shutdown(self) -> None:
        """Closes redis connection."""
        await self.redis_pool.disconnect()
        await super().shutdown()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Sets task result in redis.

        Dumps TaskiqResult instance into the bytes and writes
        it to redis.

        :param task_id: ID of the task.
        :param result: TaskiqResult instance.
        """
        redis_set_params: Dict[str, Union[str, int, bytes]] = {
            "name": self._task_name(task_id),
            "value": self.serializer.dumpb(model_dump(result)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        async with Redis(connection_pool=self.redis_pool) as redis:
            await redis.set(**redis_set_params)

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        async with Redis(connection_pool=self.redis_pool) as redis:
            return bool(await redis.exists(self._task_name(task_id)))

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: task's return value.
        """
        task_name = self._task_name(task_id)
        async with Redis(connection_pool=self.redis_pool) as redis:
            if self.keep_results:
                result_value = await redis.get(
                    name=task_name,
                )
            else:
                result_value = await redis.getdel(
                    name=task_name,
                )

        if result_value is None:
            raise ResultIsMissingError

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(result_value),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[_ReturnType],
    ) -> None:
        """
        Sets task progress in redis.

        Dumps TaskProgress instance into the bytes and writes
        it to redis with a standard suffix on the task_id as the key

        :param task_id: ID of the task.
        :param result: task's TaskProgress instance.
        """
        redis_set_params: Dict[str, Union[str, int, bytes]] = {
            "name": self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            "value": self.serializer.dumpb(model_dump(progress)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        async with Redis(connection_pool=self.redis_pool) as redis:
            await redis.set(**redis_set_params)

    async def get_progress(
        self,
        task_id: str,
    ) -> Union[TaskProgress[_ReturnType], None]:
        """
        Gets progress results from the task.

        :param task_id: task's id.
        :return: task's TaskProgress instance.
        """
        async with Redis(connection_pool=self.redis_pool) as redis:
            result_value = await redis.get(
                name=self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            )

        if result_value is None:
            return None

        return model_validate(
            TaskProgress[_ReturnType],
            self.serializer.loadb(result_value),
        )


class RedisAsyncClusterResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result backend based on redis cluster."""

    def __init__(
        self,
        redis_url: str,
        keep_results: bool = True,
        result_ex_time: Optional[int] = None,
        result_px_time: Optional[int] = None,
        serializer: Optional[TaskiqSerializer] = None,
        prefix_str: Optional[str] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new result backend.

        :param redis_url: url to redis cluster.
        :param keep_results: flag to not remove results from Redis after reading.
        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        :param connection_kwargs: additional arguments for RedisCluster.

        :raises DuplicateExpireTimeSelectedError: if result_ex_time
            and result_px_time are selected.
        :raises ExpireTimeMustBeMoreThanZeroError: if result_ex_time
            and result_px_time are equal zero.
        """
        self.redis: "RedisCluster" = RedisCluster.from_url(
            redis_url,
            **connection_kwargs,
        )
        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.prefix_str = prefix_str

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError

    def _task_name(self, task_id: str) -> str:
        if self.prefix_str is None:
            return task_id
        return f"{self.prefix_str}:{task_id}"

    async def shutdown(self) -> None:
        """Closes redis connection."""
        await self.redis.aclose()
        await super().shutdown()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Sets task result in redis.

        Dumps TaskiqResult instance into the bytes and writes
        it to redis.

        :param task_id: ID of the task.
        :param result: TaskiqResult instance.
        """
        redis_set_params: Dict[str, Union[str, bytes, int]] = {
            "name": self._task_name(task_id),
            "value": self.serializer.dumpb(model_dump(result)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        await self.redis.set(**redis_set_params)  # type: ignore

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        return bool(await self.redis.exists(self._task_name(task_id)))

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: task's return value.
        """
        task_name = self._task_name(task_id)
        if self.keep_results:
            result_value = await self.redis.get(
                name=task_name,
            )
        else:
            result_value = await self.redis.getdel(
                name=task_name,
            )

        if result_value is None:
            raise ResultIsMissingError

        taskiq_result: TaskiqResult[_ReturnType] = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(result_value),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[_ReturnType],
    ) -> None:
        """
        Sets task progress in redis.

        Dumps TaskProgress instance into the bytes and writes
        it to redis with a standard suffix on the task_id as the key

        :param task_id: ID of the task.
        :param result: task's TaskProgress instance.
        """
        redis_set_params: Dict[str, Union[str, int, bytes]] = {
            "name": self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            "value": self.serializer.dumpb(model_dump(progress)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        await self.redis.set(**redis_set_params)  # type: ignore

    async def get_progress(
        self,
        task_id: str,
    ) -> Union[TaskProgress[_ReturnType], None]:
        """
        Gets progress results from the task.

        :param task_id: task's id.
        :return: task's TaskProgress instance.
        """
        result_value = await self.redis.get(
            name=self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
        )

        if result_value is None:
            return None

        return model_validate(
            TaskProgress[_ReturnType],
            self.serializer.loadb(result_value),
        )


class RedisAsyncSentinelResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result based on redis sentinel."""

    def __init__(
        self,
        sentinels: List[Tuple[str, int]],
        master_name: str,
        keep_results: bool = True,
        result_ex_time: Optional[int] = None,
        result_px_time: Optional[int] = None,
        min_other_sentinels: int = 0,
        sentinel_kwargs: Optional[Any] = None,
        serializer: Optional[TaskiqSerializer] = None,
        prefix_str: Optional[str] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new result backend.

        :param sentinels: list of sentinel host and ports pairs.
        :param master_name: sentinel master name.
        :param keep_results: flag to not remove results from Redis after reading.
        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        :param max_connection_pool_size: maximum number of connections in pool.
        :param connection_kwargs: additional arguments for redis BlockingConnectionPool.

        :raises DuplicateExpireTimeSelectedError: if result_ex_time
            and result_px_time are selected.
        :raises ExpireTimeMustBeMoreThanZeroError: if result_ex_time
            and result_px_time are equal zero.
        """
        self.sentinel = Sentinel(
            sentinels=sentinels,
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )
        self.master_name = master_name
        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.prefix_str = prefix_str

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError

    def _task_name(self, task_id: str) -> str:
        if self.prefix_str is None:
            return task_id
        return f"{self.prefix_str}:{task_id}"

    @asynccontextmanager
    async def _acquire_master_conn(self) -> AsyncIterator[_Redis]:
        async with self.sentinel.master_for(self.master_name) as redis_conn:
            yield redis_conn

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Sets task result in redis.

        Dumps TaskiqResult instance into the bytes and writes
        it to redis.

        :param task_id: ID of the task.
        :param result: TaskiqResult instance.
        """
        redis_set_params: Dict[str, Union[str, bytes, int]] = {
            "name": self._task_name(task_id),
            "value": self.serializer.dumpb(model_dump(result)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        async with self._acquire_master_conn() as redis:
            await redis.set(**redis_set_params)  # type: ignore

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        async with self._acquire_master_conn() as redis:
            return bool(await redis.exists(self._task_name(task_id)))

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :return: task's return value.
        """
        task_name = self._task_name(task_id)
        async with self._acquire_master_conn() as redis:
            if self.keep_results:
                result_value = await redis.get(
                    name=task_name,
                )
            else:
                result_value = await redis.getdel(
                    name=task_name,
                )

        if result_value is None:
            raise ResultIsMissingError

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(result_value),
        )

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[_ReturnType],
    ) -> None:
        """
        Sets task progress in redis.

        Dumps TaskProgress instance into the bytes and writes
        it to redis with a standard suffix on the task_id as the key

        :param task_id: ID of the task.
        :param result: task's TaskProgress instance.
        """
        redis_set_params: Dict[str, Union[str, int, bytes]] = {
            "name": self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            "value": self.serializer.dumpb(model_dump(progress)),
        }
        if self.result_ex_time:
            redis_set_params["ex"] = self.result_ex_time
        elif self.result_px_time:
            redis_set_params["px"] = self.result_px_time

        async with self._acquire_master_conn() as redis:
            await redis.set(**redis_set_params)  # type: ignore

    async def get_progress(
        self,
        task_id: str,
    ) -> Union[TaskProgress[_ReturnType], None]:
        """
        Gets progress results from the task.

        :param task_id: task's id.
        :return: task's TaskProgress instance.
        """
        async with self._acquire_master_conn() as redis:
            result_value = await redis.get(
                name=self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            )

        if result_value is None:
            return None

        return model_validate(
            TaskProgress[_ReturnType],
            self.serializer.loadb(result_value),
        )

    async def shutdown(self) -> None:
        """Shutdown sentinel connections."""
        for sentinel in self.sentinel.sentinels:
            await sentinel.aclose()
