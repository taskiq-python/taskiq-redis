from typing import Any, AsyncGenerator

from redis.asyncio import RedisCluster
from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage


class BaseRedisClusterBroker(AsyncBroker):
    """Base broker that works with Redis Cluster."""

    def __init__(
        self,
        url: str,
        queue_name: str = "taskiq",
        max_connection_pool_size: int = 2**31,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker.

        :param url: url to redis.
        :param queue_name: name for a list in redis.
        :param max_connection_pool_size: maximum number of connections in pool.
        :param connection_kwargs: additional arguments for aio-redis ConnectionPool.
        """
        super().__init__()

        self.redis: RedisCluster[bytes] = RedisCluster.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )

        self.queue_name = queue_name

    async def shutdown(self) -> None:
        """Closes redis connection pool."""
        await self.redis.aclose()  # type: ignore[attr-defined]
        await super().shutdown()


class ListQueueClusterBroker(BaseRedisClusterBroker):
    """Broker that works with Redis Cluster and distributes tasks between workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        await self.redis.lpush(self.queue_name, message.message)  # type: ignore[attr-defined]

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the queue
        and yields new messages if they have BrokerMessage type.

        :yields: broker messages.
        """
        redis_brpop_data_position = 1
        while True:
            value = await self.redis.brpop([self.queue_name])  # type: ignore[attr-defined]
            yield value[redis_brpop_data_position]
