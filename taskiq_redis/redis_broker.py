import pickle
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Optional, TypeVar

from redis.asyncio import ConnectionPool, Redis
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111

logger = getLogger("taskiq.redis_broker")


class RedisBroker(AsyncBroker):
    """Broker that works with Redis."""

    def __init__(
        self,
        url: Optional[str] = None,
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

        self.redis_pubsub_channel = queue_name

    async def shutdown(self) -> None:
        """Closes redis connection pool."""
        self.connection_pool.disconnect()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Sends a message to the redis broker list.

        This function constructs message for redis
        and sends it.

        The message is pickled dict object with message,
        task_id, task_name and labels.

        :param message: message to send.
        """
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.publish(
                self.redis_pubsub_channel,
                pickle.dumps(message),
            )

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        """
        Listen redis list for new messages.

        This function listens to list and yields new messages.

        :yields: parsed broker messages.
        """
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            redis_pubsub_channel = redis_conn.pubsub()
            await redis_pubsub_channel.subscribe(self.redis_pubsub_channel)
            while True:
                redis_pickled_message = await redis_pubsub_channel.get_message()
                if redis_pickled_message:
                    try:
                        redis_message = pickle.loads(
                            redis_pickled_message["data"],
                        )
                        if isinstance(redis_message, BrokerMessage):
                            yield redis_message
                    except (
                        TypeError,
                        AttributeError,
                        pickle.UnpicklingError,
                    ) as exc:
                        logger.debug(
                            "Cannot read broker message %s",
                            exc,
                            exc_info=True,
                        )
