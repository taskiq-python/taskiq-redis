import sys
from contextlib import asynccontextmanager
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from redis.asyncio import Redis, Sentinel
from taskiq import AsyncResultBackend, BrokerMessage
from taskiq.abc.broker import AsyncBroker

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    _Redis: TypeAlias = Redis[bytes]
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
    """Broker that works with Redis and broadcasts tasks to all workers."""

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
            await redis_conn.lpush(queue_name, message.message)

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
                yield (await redis_conn.brpop(self.queue_name))[
                    redis_brpop_data_position
                ]
