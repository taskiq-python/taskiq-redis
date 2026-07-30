import asyncio
import time
import uuid
import warnings
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import suppress
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    TypeAlias,
    TypeVar,
    cast,
)

from redis.asyncio import BlockingConnectionPool, Connection, Redis, ResponseError
from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.redis_broker")

ABANDONED_CONSUMER = "abandoned"
ABANDONED_IDLE_MS = 10**12


if TYPE_CHECKING:
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore
else:
    _BlockingConnectionPool: TypeAlias = BlockingConnectionPool


class BaseRedisBroker(AsyncBroker):
    """Base broker that works with Redis."""

    def __init__(
        self,
        url: str,
        task_id_generator: Callable[[], str] | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        queue_name: str = "taskiq",
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker.

        :param url: url to redis.
        :param task_id_generator: custom task_id generator.
        :param result_backend: custom result backend.
        :param queue_name: name for a list in redis.
        :param max_connection_pool_size: maximum number of connections in pool.
            Each worker opens its own connection. Therefore this value has to be
            at least number of workers + 1.
        :param connection_kwargs: additional arguments for redis BlockingConnectionPool.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )

        self.connection_pool: _BlockingConnectionPool = BlockingConnectionPool.from_url(
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
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.publish(queue_name, message.message)

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
        queue_name = message.labels.get("queue_name") or self.queue_name
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.lpush(queue_name, message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen redis queue for new messages.

        This function listens to the queue
        and yields new messages if they have BrokerMessage type.

        :yields: broker messages.
        """
        redis_brpop_data_position = 1
        while True:
            try:
                async with Redis(connection_pool=self.connection_pool) as redis_conn:
                    brpop_result = await redis_conn.brpop(self.queue_name)
                    if brpop_result is None:
                        continue
                    yield brpop_result[redis_brpop_data_position]  # type: ignore[misc]
            except ConnectionError as exc:
                logger.warning("Redis connection error: %s", exc)
                continue


class RedisStreamBroker(BaseRedisBroker):
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
        max_connection_pool_size: int | None = None,
        consumer_group_name: str = "taskiq",
        consumer_name: str | None = None,
        consumer_id: str = "$",
        mkstream: bool = True,
        xread_block: int = 2000,
        maxlen: int | None = None,
        approximate: bool = True,
        idle_timeout: int = 600000,  # 10 minutes
        unacknowledged_batch_size: int = 100,
        unacknowledged_lock_timeout: float | None = None,
        xread_count: int | None = 100,
        reclaim_interval: int = 30000,
        reclaim_timeout_grace: int = 10000,
        additional_streams: dict[str, str | int] | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """
        Constructs a new broker that uses streams.

        :param url: url to redis.
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
        :param xread_count: number of messages to fetch from the stream at once.
            For a single-stream broker, it also caps this listener's delivered
            but unacknowledged messages. Set to None to disable this limit.
        :param additional_streams: additional streams to read from.
            Each key is a stream name, value is a consumer id. Deprecated:
            use one broker and worker process per stream instead.
        :param unacknowledged_batch_size: number of unacknowledged messages to fetch.
        :param unacknowledged_lock_timeout: deprecated and ignored. Redis' XCLAIM
            min-idle-time check is used instead of a broker-side lock.
        :param reclaim_interval: milliseconds between timed-out message scans.
            Set to 0 to scan on every listen iteration.
        :param reclaim_timeout_grace: extra time in milliseconds added to
            message's timeout label before it can be reclaimed. Messages without
            a timeout label still use idle_timeout.
        """
        super().__init__(
            url,
            task_id_generator=None,
            result_backend=None,
            queue_name=queue_name,
            max_connection_pool_size=max_connection_pool_size,
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
        if self.additional_streams:
            warnings.warn(
                "additional_streams is deprecated and will be removed in a "
                "future major release. Use one RedisStreamBroker and worker "
                "process per stream instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.idle_timeout = idle_timeout
        self.unacknowledged_batch_size = unacknowledged_batch_size
        self.unacknowledged_lock_timeout = unacknowledged_lock_timeout
        self.count = xread_count
        self.reclaim_interval = reclaim_interval
        self.reclaim_timeout_grace = reclaim_timeout_grace

    def _get_available_message_count(self, unacked: int) -> int | None:
        """Return how many more messages this listener can reserve."""
        if self.count is None:
            return None
        return max(0, self.count - unacked)

    def _should_reclaim(self, last_reclaim: float) -> bool:
        """Return whether the periodic pending-message sweep should run now."""
        if self.reclaim_interval <= 0:
            return True
        return time.monotonic() - last_reclaim >= self.reclaim_interval / 1000

    @staticmethod
    def _to_str(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _message_key(self, stream: Any, message_id: Any) -> tuple[str, str]:
        """Build a stable identity for one stream entry."""
        return self._to_str(stream), self._to_str(message_id)

    @staticmethod
    def _group_delivered_by_stream(
        delivered: set[tuple[str, str]],
    ) -> dict[str, list[str]]:
        """Group listener-held message ids by their Redis stream key."""
        grouped: dict[str, list[str]] = {}
        for stream, message_id in delivered:
            grouped.setdefault(stream, []).append(message_id)
        return grouped

    async def _declare_consumer_group(self) -> None:
        """
        Declare consumber group.

        Required for proper work of the broker.
        """
        streams = {self.queue_name, *self.additional_streams.keys()}
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
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
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.xadd(
                queue_name,
                {b"data": message.message},
                maxlen=self.maxlen,
                approximate=self.approximate,
            )

    def _ack_generator(
        self,
        id: str,
        queue_name: str,
        on_ack: Callable[[], None] | None = None,
    ) -> Callable[[], Awaitable[None]]:
        acked = False

        async def _ack() -> None:
            nonlocal acked
            async with Redis(connection_pool=self.connection_pool) as redis_conn:
                try:
                    await redis_conn.xack(
                        queue_name,
                        self.consumer_group_name,
                        id,
                    )
                finally:
                    if not acked and on_ack is not None:
                        acked = True
                        on_ack()

        return _ack

    def _get_message_reclaim_timeout(self, message: dict[bytes, bytes]) -> int:
        """Resolve how long this message may stay pending before reclaim.

        Taskiq stores execution timeout in the serialized task message labels.
        If the payload cannot be decoded or has no timeout label, fall back to
        the broker-level idle_timeout to preserve legacy behavior.
        """
        raw_data = message.get(b"data")
        if raw_data is None:
            return self.idle_timeout

        with suppress(Exception):
            timeout = self.formatter.loads(raw_data).labels.get("timeout")
            if timeout is not None:
                return int(float(timeout) * 1000) + self.reclaim_timeout_grace
        return self.idle_timeout

    async def _get_pending_message(
        self,
        redis_conn: Redis,
        stream: str,
        message_id: str,
    ) -> dict[bytes, bytes] | None:
        """Fetch the stream entry body for one pending message id."""
        results = await redis_conn.xrange(
            stream,
            min=message_id,
            max=message_id,
            count=1,
        )
        if not results:
            return None
        return cast("dict[bytes, bytes]", results[0][1])

    async def _claim_timed_out_messages(
        self,
        redis_conn: Redis,
        stream: str,
        count: int,
        protected_message_ids: set[tuple[str, str]],
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Claim pending messages that exceeded their reclaim deadline.

        protected_message_ids contains entries delivered by this listener
        instance and not acked yet. We skip those to avoid re-delivering our own
        in-flight work, while still allowing a restarted worker with the same
        Redis consumer name to recover messages left by its predecessor.
        """
        try:
            pending = await redis_conn.xpending_range(
                stream,
                self.consumer_group_name,
                min="-",
                max="+",
                count=self.unacknowledged_batch_size,
                idle=0,
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            logger.info("Consumer group missing for %s, recreating", stream)
            await self._declare_consumer_group()
            return []

        claimed: list[tuple[str, dict[bytes, bytes]]] = []
        for pending_message in pending:
            if len(claimed) >= count:
                break

            message_id = pending_message["message_id"]
            if isinstance(message_id, bytes):
                message_id = message_id.decode()
            if self._message_key(stream, message_id) in protected_message_ids:
                continue

            message = await self._get_pending_message(
                redis_conn,
                stream,
                cast(str, message_id),
            )
            if message is None:
                await redis_conn.xclaim(
                    stream,
                    self.consumer_group_name,
                    self.consumer_name,
                    min_idle_time=0,
                    message_ids=[message_id],
                    justid=True,
                )
                continue

            reclaim_timeout = self._get_message_reclaim_timeout(message)
            time_since_delivered = int(
                cast(Any, pending_message.get("time_since_delivered", 0)),
            )
            if time_since_delivered < reclaim_timeout:
                continue

            # XCLAIM rechecks min_idle_time inside Redis, so concurrent workers
            # racing for the same overdue message cannot both claim it.
            result = await redis_conn.xclaim(
                stream,
                self.consumer_group_name,
                self.consumer_name,
                min_idle_time=reclaim_timeout,
                message_ids=[message_id],
            )
            claimed.extend(cast("list[tuple[str, dict[bytes, bytes]]]", result))
        return claimed

    async def _claim_available_timed_out_messages(
        self,
        redis_conn: Redis,
        count: int,
        protected_message_ids: set[tuple[str, str]],
    ) -> list[tuple[str, dict[bytes, bytes], str]]:
        """Claim overdue messages across all configured streams."""
        claimed: list[tuple[str, dict[bytes, bytes], str]] = []
        for stream in [self.queue_name, *self.additional_streams.keys()]:
            remaining_count = count - len(claimed)
            if remaining_count <= 0:
                break
            for msg_id, msg in await self._claim_timed_out_messages(
                redis_conn,
                stream,
                remaining_count,
                protected_message_ids,
            ):
                claimed.append((msg_id, msg, stream))
        return claimed

    async def _read_new_messages(
        self,
        redis_conn: Redis,
        count: int | None,
    ) -> Any:
        """Read newly added stream messages, recreating a missing group once."""
        try:
            return await redis_conn.xreadgroup(
                self.consumer_group_name,
                self.consumer_name,
                {
                    self.queue_name: ">",
                    **self.additional_streams,  # type: ignore[dict-item]
                },
                block=self.block,
                noack=False,
                count=count,
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            logger.info("Consumer group missing, recreating")
            await self._declare_consumer_group()
            return []

    async def _abandon_buffered_messages(
        self,
        redis_conn: Redis,
        buffered: list[tuple[str, dict[bytes, bytes], str]],
    ) -> None:
        """Make fetched but not yet yielded messages immediately reclaimable."""
        message_keys = {
            self._message_key(stream, msg_id) for msg_id, _, stream in buffered
        }
        grouped = self._group_delivered_by_stream(message_keys)
        for stream, message_ids in grouped.items():
            try:
                await redis_conn.xclaim(
                    stream,
                    self.consumer_group_name,
                    ABANDONED_CONSUMER,
                    min_idle_time=0,
                    message_ids=cast(Any, message_ids),
                    idle=ABANDONED_IDLE_MS,
                    justid=True,
                )
            except ResponseError as exc:
                if "NOGROUP" not in str(exc):
                    logger.warning(
                        "Failed to abandon messages in stream %s",
                        stream,
                        exc_info=True,
                    )

    def _build_ackable_message(
        self,
        msg_id: str,
        msg: dict[bytes, bytes],
        queue_name: str,
        on_ack: Callable[[], None] | None = None,
    ) -> AckableMessage:
        return AckableMessage(
            data=msg[b"data"],
            ack=self._ack_generator(id=msg_id, queue_name=queue_name, on_ack=on_ack),
        )

    def _build_ackable_messages(
        self,
        messages: Iterable[tuple[str, dict[bytes, bytes], str]],
        make_on_ack: Callable[[tuple[str, str]], Callable[[], None]],
    ) -> list[AckableMessage]:
        """Convert Redis stream entries to AckableMessage objects.

        The caller adds each entry to the listener-local delivered set right
        before yielding it, because entries fetched but not yielded yet can be
        abandoned immediately on generator close.
        """
        ackable_messages = []
        for msg_id, msg, stream in messages:
            logger.debug("Received message: %s", msg)
            message_key = self._message_key(stream, msg_id)
            ackable_messages.append(
                self._build_ackable_message(
                    msg_id=msg_id,
                    msg=msg,
                    queue_name=stream,
                    on_ack=make_on_ack(message_key),
                ),
            )
        return ackable_messages

    @staticmethod
    def _flatten_fetched_messages(
        fetched: Any,
    ) -> list[tuple[str, dict[bytes, bytes], str]]:
        """Normalize XREADGROUP's grouped response to (id, body, stream) tuples."""
        messages = []
        for stream, msg_list in fetched:
            for msg_id, msg in msg_list:
                messages.append((msg_id, msg, stream))
        return messages

    async def _build_reclaimed_ackable_messages(
        self,
        redis_conn: Redis,
        count: int,
        delivered: set[tuple[str, str]],
    ) -> list[tuple[str, dict[bytes, bytes], str]]:
        """Claim overdue messages for taskiq's receiver."""
        return await self._claim_available_timed_out_messages(
            redis_conn,
            count or self.unacknowledged_batch_size,
            delivered,
        )

    async def _build_due_reclaimed_messages(
        self,
        redis_conn: Redis,
        count: int,
        delivered: set[tuple[str, str]],
        last_reclaim: float,
    ) -> tuple[float, list[tuple[str, dict[bytes, bytes], str]]]:
        """Return overdue pending messages only when the reclaim interval elapsed."""
        if not self._should_reclaim(last_reclaim):
            return last_reclaim, []
        return time.monotonic(), await self._build_reclaimed_ackable_messages(
            redis_conn,
            count,
            delivered,
        )

    async def _build_new_ackable_messages(
        self,
        redis_conn: Redis,
        count: int | None,
    ) -> list[tuple[str, dict[bytes, bytes], str]]:
        """Read fresh stream messages for taskiq's receiver."""
        fetched = await self._read_new_messages(redis_conn, count)
        if not fetched:
            return []
        return self._flatten_fetched_messages(fetched)

    async def _yield_buffered_messages(
        self,
        buffered: list[tuple[str, dict[bytes, bytes], str]],
        delivered: set[tuple[str, str]],
        make_on_ack: Callable[[tuple[str, str]], Callable[[], None]],
    ) -> AsyncGenerator[AckableMessage, None]:
        """Yield fetched messages and keep only not-yielded entries in buffer."""
        messages = self._build_ackable_messages(buffered, make_on_ack)
        while buffered and messages:
            msg_id, _, stream = buffered.pop(0)
            message = messages.pop(0)
            delivered.add(self._message_key(stream, msg_id))
            yield message

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Listen to incoming messages with local prefetch/backpressure."""
        unacked = 0
        # Only entries delivered by this listener instance are protected from
        # reclaim. Reusing the same Redis consumer_name after a restart is still
        # recoverable because the new listener starts with an empty set.
        delivered: set[tuple[str, str]] = set()
        buffered: list[tuple[str, dict[bytes, bytes], str]] = []
        slot_freed = asyncio.Event()
        last_reclaim = 0.0

        def on_ack(message_key: tuple[str, str]) -> None:
            nonlocal unacked
            delivered.discard(message_key)
            unacked = max(0, unacked - 1)
            slot_freed.set()

        def make_on_ack(message_key: tuple[str, str]) -> Callable[[], None]:
            def _on_ack() -> None:
                on_ack(message_key)

            return _on_ack

        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            try:
                while True:
                    count = self._get_available_message_count(unacked)
                    if count == 0:
                        # Do not reserve more stream entries while all local
                        # prefetch slots are occupied.
                        await slot_freed.wait()
                        slot_freed.clear()
                        continue
                    last_reclaim, buffered = await self._build_due_reclaimed_messages(
                        redis_conn,
                        count or self.unacknowledged_batch_size,
                        delivered,
                        last_reclaim,
                    )
                    unacked += len(buffered)
                    async for message in self._yield_buffered_messages(
                        buffered,
                        delivered,
                        make_on_ack,
                    ):
                        yield message
                    count = self._get_available_message_count(unacked)
                    if count != 0:
                        logger.debug("Starting fetching new messages")
                        buffered = await self._build_new_ackable_messages(
                            redis_conn,
                            count,
                        )
                        unacked += len(buffered)
                        async for message in self._yield_buffered_messages(
                            buffered,
                            delivered,
                            make_on_ack,
                        ):
                            yield message
            finally:
                if buffered:
                    await self._abandon_buffered_messages(redis_conn, buffered)
