import pickle
from typing import TypeVar

from redis.asyncio import ConnectionPool, Redis
from taskiq import AsyncResultBackend
from taskiq.abc.result_backend import TaskiqResult

_ReturnType = TypeVar("_ReturnType")


class RedisAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result based on redis."""

    def __init__(self, redis_url: str, keep_results: bool = True):
        """
        Constructs a new result backend.

        :param redis_url: url to redis.
        :param keep_results: flag to not remove results from Redis after reading.
        """
        self.redis_pool = ConnectionPool.from_url(redis_url)
        self.keep_results = keep_results

    async def shutdown(self) -> None:
        """Closes redis connection."""
        await self.redis_pool.disconnect()

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
        result_dict = result.dict(exclude={"return_value"})

        for result_key, result_value in result_dict.items():
            result_dict[result_key] = pickle.dumps(result_value)
        # This trick will preserve original returned value.
        # It helps when you return not serializable classes.
        result_dict["return_value"] = pickle.dumps(result.return_value)

        async with Redis(connection_pool=self.redis_pool) as redis:
            await redis.hset(
                task_id,
                mapping=result_dict,
            )

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        async with Redis(connection_pool=self.redis_pool) as redis:
            return bool(await redis.exists(task_id))

    async def get_result(  # noqa: WPS210
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :return: task's return value.
        """
        fields = list(TaskiqResult.__fields__.keys())

        if not with_logs:
            fields.remove("log")

        async with Redis(connection_pool=self.redis_pool) as redis:
            result_values = await redis.hmget(
                name=task_id,
                keys=fields,
            )

            if not self.keep_results:
                await redis.delete(task_id)

        result = {
            result_key: pickle.loads(result_value)
            for result_value, result_key in zip(result_values, fields)
        }

        return TaskiqResult(**result)
