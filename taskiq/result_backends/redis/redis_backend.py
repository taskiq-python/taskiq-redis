import pickle
from typing import Any, Dict, TypeVar

from redis.asyncio import ConnectionPool, Redis
from taskiq.abc.result_backend import TaskiqResult

from taskiq import AsyncResultBackend

_ReturnType = TypeVar("_ReturnType")


class RedisAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result based on redis."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url

    async def startup(self) -> None:
        """Makes redis connection on startup."""
        self.redis_pool = ConnectionPool.from_url(self.redis_url)

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
        result_dict = result.dict()

        for result_key, result_value in result_dict.items():
            result_dict[result_key] = pickle.dumps(result_value)

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
            return await redis.exists(task_id)

    async def get_result(
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
        result: Dict[str, Any] = {
            result_key: None for result_key in TaskiqResult.__fields__
        }

        if not with_logs:
            result.pop("log")

        async with Redis(connection_pool=self.redis_pool) as redis:
            result_values = await redis.hmget(
                name=task_id,
                keys=result,
            )

        for result_value, result_key in zip(result_values, result):
            result[result_key] = pickle.loads(result_value)

        return TaskiqResult(**result)


async def main():
    t = TaskiqResult(
        is_err=False,
        log="ASD",
        return_value=123,
        execution_time=1.0,
    )

    r = RedisAsyncResultBackend("redis://localhost:6379")
    await r.set_result()