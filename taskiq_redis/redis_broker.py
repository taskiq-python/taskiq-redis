from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Optional, TypeVar

from redis.asyncio import ConnectionPool, Redis
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.redis_broker")


class BaseRedisBroker(AsyncBroker):
    """Base broker that works with Redis."""

    def __init__(
        self,
        url: str,
        task_id_generator: Optional[Callable[[], str]] = None,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        queue_name: str = "taskiq",
        max_connection_pool_size: Optional[int] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker.

        :param url: url to redis.
        :param task_id_generator: custom task_id generator.
        :param result_backend: custom result backend.
        :param queue_name: name for a list in redis.
        :param max_connection_pool_size: maximum number of connections in pool.
        :param connection_kwargs: additional arguments for aio-redis ConnectionPool.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )

        self.connection_pool: ConnectionPool = ConnectionPool.from_url(
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.queue_name = queue_name

    async def shutdown(self) -> None:
        """Closes redis connection pool."""
        await super().shutdown()
        await self.connection_pool.disconnect()


class PubSubBroker(BaseRedisBroker):
    """Broker that works with Redis and broadcasts tasks to all workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Publish message over PUBSUB channel.

        :param message: message to send.
        """
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.publish(self.queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the pubsub channel
        and yields all messages with proper types.

        :yields: broker messages.
        """
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            redis_pubsub_channel = redis_conn.pubsub()
            await redis_pubsub_channel.subscribe(self.queue_name)
            async for message in redis_pubsub_channel.listen():
                if not message:
                    continue
                if message["type"] != "message":
                    logger.debug("Received non-message from redis: %s", message)
                    continue
                yield message["data"]


class ListQueueBroker(BaseRedisBroker):
    """Broker that works with Redis and distributes tasks between workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.lpush(self.queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the queue
        and yields new messages if they have BrokerMessage type.

        :yields: broker messages.
        """
        redis_brpop_data_position = 1
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            while True:
                yield (await redis_conn.brpop(self.queue_name))[
                    redis_brpop_data_position
                ]
