import uuid
from logging import getLogger
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Optional

from redis.asyncio import RedisCluster, ResponseError
from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage

logger = getLogger(__name__)


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

        self.redis: "RedisCluster[bytes]" = RedisCluster.from_url(  # type: ignore
            url=url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )

        self.queue_name = queue_name

    async def shutdown(self) -> None:
        """Closes redis connection pool."""
        await self.redis.aclose()
        await super().shutdown()


class ListQueueClusterBroker(BaseRedisClusterBroker):
    """Broker that works with Redis Cluster and distributes tasks between workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        await self.redis.lpush(self.queue_name, message.message)  # type: ignore

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the queue
        and yields new messages if they have BrokerMessage type.

        :yields: broker messages.
        """
        redis_brpop_data_position = 1
        while True:
            value = await self.redis.brpop([self.queue_name])  # type: ignore
            yield value[redis_brpop_data_position]


class RedisStreamClusterBroker(BaseRedisClusterBroker):
    """
    Redis broker that uses streams for task distribution.

    You can read more about streams here:
    https://redis.io/docs/latest/develop/data-types/streams

    This broker supports acknowledgment of messages.
    """

    def __init__(
        self,
        url: str,
        queue_name: str = "taskiq",
        max_connection_pool_size: int = 2**31,
        consumer_group_name: str = "taskiq",
        consumer_name: Optional[str] = None,
        consumer_id: str = "$",
        mkstream: bool = True,
        xread_block: int = 10000,
        additional_streams: Optional[Dict[str, str]] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(
            url,
            queue_name=queue_name,
            max_connection_pool_size=max_connection_pool_size,
            **connection_kwargs,
        )
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self.consumer_id = consumer_id
        self.mkstream = mkstream
        self.block = xread_block
        self.additional_streams = additional_streams or {}

    async def _declare_consumer_group(self) -> None:
        streams = {self.queue_name, *self.additional_streams.keys()}
        async with self.redis as redis_conn:
            for stream_name in streams:
                try:
                    await redis_conn.xgroup_create(
                        stream_name,
                        self.consumer_group_name,
                        id=self.consumer_id,
                        mkstream=self.mkstream,
                    )
                except ResponseError as err:
                    logger.debug(err)

    async def startup(self) -> None:
        """Declare consumer group on startup."""
        await super().startup()
        await self._declare_consumer_group()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        await self.redis.xadd(self.queue_name, {b"data": message.message})

    def _ack_generator(self, id: str) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            await self.redis.xack(
                self.queue_name,
                self.consumer_group_name,
                id,
            )

        return _ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Listen to the stream for new messages."""
        while True:
            fetched = await self.redis.xreadgroup(
                self.consumer_group_name,
                self.consumer_name,
                {
                    self.queue_name: ">",
                    **self.additional_streams,  # type: ignore
                },
                block=self.block,
                noack=False,
            )
            for _, msg_list in fetched:
                for msg_id, msg in msg_list:
                    logger.debug("Received message: %s", msg)
                    yield AckableMessage(
                        data=msg[b"data"],
                        ack=self._ack_generator(msg_id),
                    )
