import asyncio
import uuid

import pytest
from redis.asyncio import Redis
from taskiq import AckableMessage, AsyncBroker, BrokerMessage
from taskiq.message import TaskiqMessage

from taskiq_redis import (
    ListQueueBroker,
    ListQueueClusterBroker,
    ListQueueSentinelBroker,
    PubSubBroker,
    PubSubSentinelBroker,
    RedisStreamClusterBroker,
    RedisStreamSentinelBroker,
)
from taskiq_redis.redis_broker import ABANDONED_CONSUMER, RedisStreamBroker


def test_no_url_should_raise_typeerror() -> None:
    """Test that url is expected."""
    with pytest.raises(TypeError):
        ListQueueBroker()  # type: ignore


async def get_message(
    broker: AsyncBroker,
) -> bytes | AckableMessage:
    """
    Get a message from the broker.

    :param broker: async message broker.
    :return: first message from listen method.
    """
    async for message in broker.listen():
        return message
    return b""


@pytest.fixture
def valid_broker_message() -> BrokerMessage:
    """
    Generate valid broker message for tests.

    :returns: broker message.
    """
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )


@pytest.mark.anyio
async def test_pub_sub_broker(
    valid_broker_message: BrokerMessage,
    redis_url: str,
) -> None:
    """
    Test that messages are published and read correctly by PubSubBroker.

    We create two workers that listen and send a message to them.
    Expect both workers to receive the same message we sent.
    """
    broker = PubSubBroker(url=redis_url, queue_name=uuid.uuid4().hex)
    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    message1 = worker1_task.result()
    message2 = worker2_task.result()
    assert message1 == valid_broker_message.message
    assert message1 == message2
    await broker.shutdown()


@pytest.mark.anyio
async def test_pub_sub_broker_max_connections(
    valid_broker_message: BrokerMessage,
    redis_url: str,
) -> None:
    """Test PubSubBroker with connection limit set."""
    broker = PubSubBroker(
        url=redis_url,
        queue_name=uuid.uuid4().hex,
        max_connection_pool_size=4,
        timeout=1,
    )
    worker_tasks = [asyncio.create_task(get_message(broker)) for _ in range(3)]
    await asyncio.sleep(0.3)

    await asyncio.gather(*[broker.kick(valid_broker_message) for _ in range(50)])
    await asyncio.sleep(0.3)

    for worker in worker_tasks:
        worker.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_list_queue_broker(
    valid_broker_message: BrokerMessage,
    redis_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ListQueueBroker(url=redis_url, queue_name=uuid.uuid4().hex)
    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    assert worker1_task.done() != worker2_task.done()
    message = worker1_task.result() if worker1_task.done() else worker2_task.result()
    assert message == valid_broker_message.message
    worker1_task.cancel()
    worker2_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker(
    valid_broker_message: BrokerMessage,
    redis_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = RedisStreamBroker(
        url=redis_url,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()

    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    await asyncio.wait(
        [worker1_task, worker2_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    assert worker1_task.done() != worker2_task.done()
    message = worker1_task.result() if worker1_task.done() else worker2_task.result()
    assert isinstance(message, AckableMessage)
    assert message.data == valid_broker_message.message
    await message.ack()  # type: ignore
    worker1_task.cancel()
    worker2_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_list_queue_broker_max_connections(
    valid_broker_message: BrokerMessage,
    redis_url: str,
) -> None:
    """Test ListQueueBroker with connection limit set."""
    broker = ListQueueBroker(
        url=redis_url,
        queue_name=uuid.uuid4().hex,
        max_connection_pool_size=4,
        timeout=1,
    )
    worker_tasks = [asyncio.create_task(get_message(broker)) for _ in range(3)]
    await asyncio.sleep(0.3)

    await asyncio.gather(*[broker.kick(valid_broker_message) for _ in range(50)])
    await asyncio.sleep(0.3)

    for worker in worker_tasks:
        worker.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_list_queue_cluster_broker(
    valid_broker_message: BrokerMessage,
    redis_cluster_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueClusterBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ListQueueClusterBroker(
        url=redis_cluster_url,
        queue_name=uuid.uuid4().hex,
    )
    worker_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    assert worker_task.done()
    assert worker_task.result() == valid_broker_message.message
    worker_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_stream_cluster_broker(
    valid_broker_message: BrokerMessage,
    redis_cluster_url: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueClusterBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = RedisStreamClusterBroker(
        url=redis_cluster_url,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()

    worker_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    result = await worker_task

    assert isinstance(result, AckableMessage)
    assert result.data == valid_broker_message.message
    await result.ack()  # type: ignore
    worker_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_pub_sub_sentinel_broker(
    valid_broker_message: BrokerMessage,
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by PubSubSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect both workers to receive the same message we sent.
    """
    broker = PubSubSentinelBroker(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
    )
    worker1_task = asyncio.create_task(get_message(broker))
    worker2_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    message1 = worker1_task.result()
    message2 = worker2_task.result()
    assert message1 == valid_broker_message.message
    assert message1 == message2
    await broker.shutdown()


@pytest.mark.anyio
async def test_list_queue_sentinel_broker(
    valid_broker_message: BrokerMessage,
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by ListQueueSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = ListQueueSentinelBroker(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
    )
    worker_task = asyncio.create_task(get_message(broker))
    await asyncio.sleep(0.3)

    await broker.kick(valid_broker_message)
    await asyncio.sleep(0.3)

    assert worker_task.done()
    assert worker_task.result() == valid_broker_message.message
    worker_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_streams_sentinel_broker(
    valid_broker_message: BrokerMessage,
    redis_sentinels: list[tuple[str, int]],
    redis_sentinel_master_name: str,
) -> None:
    """
    Test that messages are published and read correctly by RedisStreamSentinelBroker.

    We create two workers that listen and send a message to them.
    Expect only one worker to receive the same message we sent.
    """
    broker = RedisStreamSentinelBroker(
        sentinels=redis_sentinels,
        master_name=redis_sentinel_master_name,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )
    await broker.startup()
    worker_task = asyncio.create_task(get_message(broker))

    await broker.kick(valid_broker_message)

    result = await worker_task
    assert isinstance(result, AckableMessage)
    assert result.data == valid_broker_message.message
    await result.ack()  # type: ignore
    worker_task.cancel()
    await broker.shutdown()


@pytest.mark.anyio
async def test_maxlen_in_stream_broker(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """
    Test that maxlen parameter works correctly in RedisStreamBroker.

    We create RedisStreamBroker, fill in them with messages in the amount of
    > maxlen and check that only maxlen messages are in the stream.
    """
    maxlen = 20

    broker = RedisStreamBroker(
        url=redis_url,
        maxlen=maxlen,
        approximate=False,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )

    await broker.startup()

    for _ in range(maxlen * 2):
        await broker.kick(valid_broker_message)

    async with Redis(connection_pool=broker.connection_pool) as redis:
        assert await redis.xlen(broker.queue_name) == maxlen
    await broker.shutdown()


@pytest.mark.anyio
async def test_maxlen_in_cluster_stream_broker(
    redis_cluster_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """
    Test that maxlen parameter works correctly in RedisStreamClusterBroker.

    We create RedisStreamClusterBroker, fill it with messages in the amount of
    > maxlen and check that only maxlen messages are in the stream.
    """
    maxlen = 20

    broker = RedisStreamClusterBroker(
        maxlen=maxlen,
        approximate=False,
        url=redis_cluster_url,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
    )

    await broker.startup()

    for _ in range(maxlen * 2):
        await broker.kick(valid_broker_message)

    assert await broker.redis.xlen(broker.queue_name) == maxlen
    await broker.shutdown()


@pytest.mark.anyio
async def test_maxlen_in_sentinel_stream_broker(
    redis_sentinel_master_name: str,
    redis_sentinels: list[tuple[str, int]],
    valid_broker_message: BrokerMessage,
) -> None:
    """
    Test that maxlen parameter works correctly in RedisStreamSentinelBroker.

    We create RedisStreamSentinelBroker, fill it with messages in the amount of
    > maxlen and check that only maxlen messages are in the stream.
    """
    maxlen = 20

    broker = RedisStreamSentinelBroker(
        maxlen=maxlen,
        approximate=False,
        sentinels=redis_sentinels,
        queue_name=uuid.uuid4().hex,
        consumer_group_name=uuid.uuid4().hex,
        master_name=redis_sentinel_master_name,
    )

    await broker.startup()

    for _ in range(maxlen * 2):
        await broker.kick(valid_broker_message)

    async with broker._acquire_master_conn() as redis_conn:
        assert await redis_conn.xlen(broker.queue_name) == maxlen
    await broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker_reclaims_messages_by_task_timeout(
    redis_url: str,
) -> None:
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex

    first_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
    )
    second_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
        reclaim_interval=0,
        reclaim_timeout_grace=0,
    )

    await first_broker.startup()
    await second_broker.startup()

    taskiq_message = TaskiqMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        labels={"timeout": 0.1},
        args=[],
        kwargs={},
    )
    broker_message = first_broker.formatter.dumps(taskiq_message)
    await first_broker.kick(broker_message)

    first_message = await get_message(first_broker)
    assert isinstance(first_message, AckableMessage)
    assert first_message.data == broker_message.message

    await asyncio.sleep(0.15)
    reclaimed_message = await asyncio.wait_for(get_message(second_broker), timeout=2)

    assert isinstance(reclaimed_message, AckableMessage)
    assert reclaimed_message.data == broker_message.message
    await reclaimed_message.ack()  # type: ignore

    await first_broker.shutdown()
    await second_broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker_unacked_message_is_reclaimed_by_idle_timeout(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """A message without a timeout label is reclaimed after idle_timeout.

    The first consumer fetches but never acks; the second consumer (with a
    small idle_timeout) reclaims it via XCLAIM.
    """
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex

    first_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
    )
    second_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
        idle_timeout=100,
        reclaim_interval=0,
        reclaim_timeout_grace=0,
    )

    await first_broker.startup()
    await second_broker.startup()
    await first_broker.kick(valid_broker_message)

    first_message = await get_message(first_broker)
    assert isinstance(first_message, AckableMessage)
    assert first_message.data == valid_broker_message.message
    # Do not ack — leave it pending for the first consumer.

    reclaimed_message = await asyncio.wait_for(get_message(second_broker), timeout=3)
    assert isinstance(reclaimed_message, AckableMessage)
    assert reclaimed_message.data == valid_broker_message.message
    await reclaimed_message.ack()  # type: ignore

    await first_broker.shutdown()
    await second_broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker_reclaims_messages_with_shared_consumer_name(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """A restarted worker can reclaim its previous consumer's pending messages."""
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex
    consumer_name = uuid.uuid4().hex

    first_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=consumer_name,
        xread_block=50,
    )
    second_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=consumer_name,
        xread_block=50,
        idle_timeout=100,
        reclaim_interval=0,
    )

    await first_broker.startup()
    await second_broker.startup()
    await first_broker.kick(valid_broker_message)

    first_message = await get_message(first_broker)
    assert isinstance(first_message, AckableMessage)
    assert first_message.data == valid_broker_message.message

    reclaimed_message = await asyncio.wait_for(get_message(second_broker), timeout=3)
    assert isinstance(reclaimed_message, AckableMessage)
    assert reclaimed_message.data == valid_broker_message.message
    await reclaimed_message.ack()  # type: ignore

    await first_broker.shutdown()
    await second_broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker_xread_count_limits_unacked_messages(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """The listener does not read more messages while at xread_count capacity."""
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex

    broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        xread_block=50,
        xread_count=1,
        reclaim_interval=0,
    )

    await broker.startup()
    await broker.kick(valid_broker_message)
    await broker.kick(valid_broker_message)

    iterator = broker.listen()
    first_message = await iterator.__anext__()
    assert isinstance(first_message, AckableMessage)

    second_task = asyncio.create_task(iterator.__anext__())
    await asyncio.sleep(0.2)
    assert not second_task.done()

    async with Redis(connection_pool=broker.connection_pool) as redis:
        pending = await redis.xpending_range(
            queue_name,
            consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
    assert len(pending) == 1

    await first_message.ack()  # type: ignore
    second_message = await asyncio.wait_for(second_task, timeout=2)
    assert isinstance(second_message, AckableMessage)
    await second_message.ack()  # type: ignore

    await iterator.aclose()
    await broker.shutdown()


def test_stream_broker_additional_streams_is_deprecated() -> None:
    """Additional streams warn users to migrate to one broker per stream."""
    with pytest.warns(
        DeprecationWarning,
        match="additional_streams is deprecated",
    ):
        RedisStreamBroker(
            "redis://localhost:7000",
            additional_streams={"secondary": ">"},
        )


@pytest.mark.anyio
async def test_stream_broker_abandons_buffered_messages_on_close(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    """Messages fetched but not yielded are handed back on generator close."""
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex

    first_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
        xread_count=2,
        reclaim_interval=0,
    )
    second_broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_name=uuid.uuid4().hex,
        xread_block=50,
        xread_count=1,
        reclaim_interval=0,
    )

    await first_broker.startup()
    await second_broker.startup()
    await first_broker.kick(valid_broker_message)
    await first_broker.kick(valid_broker_message)

    iterator = first_broker.listen()
    first_message = await iterator.__anext__()
    assert isinstance(first_message, AckableMessage)

    await iterator.aclose()

    async with Redis(connection_pool=first_broker.connection_pool) as redis:
        pending = await redis.xpending_range(
            queue_name,
            consumer_group_name,
            min="-",
            max="+",
            count=10,
        )

    pending_by_consumer = {entry["consumer"] for entry in pending}
    assert ABANDONED_CONSUMER.encode() in pending_by_consumer
    assert first_broker.consumer_name.encode() in pending_by_consumer

    reclaimed_message = await asyncio.wait_for(get_message(second_broker), timeout=2)
    assert isinstance(reclaimed_message, AckableMessage)
    assert reclaimed_message.data == valid_broker_message.message

    await first_message.ack()  # type: ignore
    await reclaimed_message.ack()  # type: ignore
    await first_broker.shutdown()
    await second_broker.shutdown()


@pytest.mark.anyio
async def test_stream_broker_recreates_missing_consumer_group(
    redis_url: str,
    valid_broker_message: BrokerMessage,
) -> None:
    queue_name = uuid.uuid4().hex
    consumer_group_name = uuid.uuid4().hex

    broker = RedisStreamBroker(
        url=redis_url,
        approximate=False,
        queue_name=queue_name,
        consumer_group_name=consumer_group_name,
        consumer_id="0",
        xread_block=50,
        reclaim_interval=0,
    )

    await broker.startup()
    async with Redis(connection_pool=broker.connection_pool) as redis:
        await redis.xgroup_destroy(queue_name, consumer_group_name)
    await broker.kick(valid_broker_message)

    message = await asyncio.wait_for(get_message(broker), timeout=2)
    assert isinstance(message, AckableMessage)
    assert message.data == valid_broker_message.message
    await message.ack()  # type: ignore

    await broker.shutdown()
