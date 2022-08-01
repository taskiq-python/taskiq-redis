import pickle
from typing import Any, Dict, TypeVar

from redis_client import RedisClient
from taskiq.abc.result_backend import TaskiqResult

from taskiq import AsyncResultBackend

_ReturnType = TypeVar("_ReturnType")


class RedisAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result based on redis."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url

    async def startup(self) -> None:
        """Makes redis connection on startup."""
        self.redis_client = RedisClient(redis_url=self.redis_url)

    async def shutdown(self) -> None:
        """Closes redis connection."""
        await self.redis_client.close()

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
        to_insert_result_data = {}

        for result_key, result_value in result.__dict__.items():
            result_value = pickle.dumps(result_value)
            to_insert_result_data[result_key] = result_value

        await self.redis_client.hset(
            task_id,
            mapping=to_insert_result_data,
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        return await self.redis_client.exists(task_id)

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
        task_data: Dict[str, Any] = {"log": None}

        redis_key_result_param = {
            "is_err": "is_err",
            "_return_value": "return_value",
            "execution_time": "execution_time",
        }

        if with_logs:
            redis_key_result_param["log"] = "log"

        for redis_key, result_param in redis_key_result_param.items():
            key_data = pickle.loads(
                await self.redis_client.hget(
                    task_id,
                    redis_key,
                ),
            )
            task_data[result_param] = key_data

        return TaskiqResult(**task_data)
