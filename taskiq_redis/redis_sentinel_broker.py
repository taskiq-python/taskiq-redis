import sys
import uuid
from contextlib import asynccontextmanager
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from redis import ResponseError
from redis.asyncio import Redis, Sentinel
from taskiq import AckableMessage, AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    _Redis: TypeAlias = Redis[bytes]  # type: ignore
else:
    _Redis: TypeAlias = Redis

_T = TypeVar("_T")

logger = getLogger("taskiq.redis_sentinel_broker")


class BaseSentinelBroker(AsyncBroker):
    """Base broker that works with Sentinel."""

    def __init__(
        self,
        sentinels: List[Tuple[str, int]],
        master_name: str,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        queue_name: str = "taskiq",
        min_other_sentinels: int = 0,
        sentinel_kwargs: Optional[Any] = None,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )

        self.sentinel = Sentinel(
            sentinels=sentinels,
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connection_kwargs,
        )
        self.master_name = master_name
        self.queue_name = queue_name

    @asynccontextmanager
    async def _acquire_master_conn(self) -> AsyncIterator[_Redis]:
        async with self.sentinel.master_for(self.master_name) as redis_conn:
            yield redis_conn


class PubSubSentinelBroker(BaseSentinelBroker):
    """Broker that works with Sentinel and broadcasts tasks to all workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Publish message over PUBSUB channel.

        :param message: message to send.
        """
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with self._acquire_master_conn() as redis_conn:
            await redis_conn.publish(queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the pubsub channel
        and yields all messages with proper types.

        :yields: broker messages.
        """
        async with self._acquire_master_conn() as redis_conn:
            redis_pubsub_channel = redis_conn.pubsub()
            await redis_pubsub_channel.subscribe(self.queue_name)
            async for message in redis_pubsub_channel.listen():
                if not message:
                    continue
                if message["type"] != "message":
                    logger.debug("Received non-message from redis: %s", message)
                    continue
                yield message["data"]


class ListQueueSentinelBroker(BaseSentinelBroker):
    """Broker that works with Sentinel and distributes tasks between workers."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        Put a message in a list.

        This method appends a message to the list of all messages.

        :param message: message to append.
        """
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with self._acquire_master_conn() as redis_conn:
            await redis_conn.lpush(queue_name, message.message)  # type: ignore

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the queue
        and yields new messages if they have BrokerMessage type.

        :yields: broker messages.
        """
        redis_brpop_data_position = 1
        async with self._acquire_master_conn() as redis_conn:
            while True:
                yield (await redis_conn.brpop(self.queue_name))[  # type: ignore
                    redis_brpop_data_position
                ]


class RedisStreamSentinelBroker(BaseSentinelBroker):
    """
    Redis broker that uses streams for task distribution.

    You can read more about streams here:
    https://redis.io/docs/latest/develop/data-types/streams

    This broker supports acknowledgment of messages.
    """

    def __init__(
        self,
        sentinels: List[Tuple[str, int]],
        master_name: str,
        min_other_sentinels: int = 0,
        queue_name: str = "taskiq",
        consumer_group_name: str = "taskiq",
        consumer_name: Optional[str] = None,
        consumer_id: str = "$",
        mkstream: bool = True,
        xread_block: int = 10000,
        maxlen: Optional[int] = None,
        approximate: bool = True,
        additional_streams: Optional[Dict[str, str]] = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker that uses streams.

        :param sentinels: list of nodes to connect to.
        :param queue_name: name for a key with stream in redis.
        :param max_connection_pool_size: maximum number of connections in pool.
            Each worker opens its own connection. Therefore this value has to be
            at least number of workers + 1.
        :param consumer_group_name: name for a consumer group.
            Redis will keep track of acked messages for this group.
        :param consumer_name: name for a consumer. By default it is a random uuid.
        :param consumer_id: id for a consumer. ID of a message to start reading from.
            $ means start from the latest message.
        :param mkstream: create stream if it does not exist.
        :param xread_block: block time in ms for xreadgroup.
            Better to set it to a bigger value, to avoid unnecessary calls.
        :param maxlen: sets the maximum length of the stream
            trims (the old values of) the stream each time a new element is added
        :param approximate: decides wether to trim the stream immediately (False) or
            later on (True)
        :param additional_streams: additional streams to read from.
            Each key is a stream name, value is a consumer id.
        """
        super().__init__(
            sentinels=sentinels,
            master_name=master_name,
            min_other_sentinels=min_other_sentinels,
            task_id_generator=None,
            result_backend=None,
            queue_name=queue_name,
            **connection_kwargs,
        )
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name or str(uuid.uuid4())
        self.consumer_id = consumer_id
        self.mkstream = mkstream
        self.block = xread_block
        self.maxlen = maxlen
        self.approximate = approximate
        self.additional_streams = additional_streams or {}

    async def _declare_consumer_group(self) -> None:
        streams = {self.queue_name, *self.additional_streams.keys()}
        async with self._acquire_master_conn() as redis_conn:
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
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with self._acquire_master_conn() as redis_conn:
            await redis_conn.xadd(
                queue_name,
                {b"data": message.message},
                maxlen=self.maxlen,
                approximate=self.approximate,
            )

    def _ack_generator(self, id: str, queue_name: str) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            async with self._acquire_master_conn() as redis_conn:
                await redis_conn.xack(
                    queue_name,
                    self.consumer_group_name,
                    id,
                )

        return _ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Listen to the stream for new messages."""
        async with self._acquire_master_conn() as redis_conn:
            while True:
                fetched = await redis_conn.xreadgroup(
                    self.consumer_group_name,
                    self.consumer_name,
                    {
                        self.queue_name: ">",
                        **self.additional_streams,  # type: ignore
                    },
                    block=self.block,
                    noack=False,
                )
                for stream, msg_list in fetched:
                    for msg_id, msg in msg_list:
                        logger.debug("Received message: %s", msg)
                        yield AckableMessage(
                            data=msg[b"data"],
                            ack=self._ack_generator(id=msg_id, queue_name=stream),
                        )
