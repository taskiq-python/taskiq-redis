import asyncio
import uuid
from typing import List, Tuple, Union

import pytest
from redis.asyncio import Redis
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from taskiq_redis import (
    ListQueueBroker,
    ListQueueClusterBroker,
    ListQueueSentinelBroker,
    PubSubBroker,
    PubSubSentinelBroker,
    RedisStreamClusterBroker,
    RedisStreamSentinelBroker,
)
from taskiq_redis.redis_broker import RedisStreamBroker


def test_no_url_should_raise_typeerror() -> None:
    """Test that url is expected."""
    with pytest.raises(TypeError):
        ListQueueBroker()  # type: ignore


async def get_message(
    broker: AsyncBroker,
) -> Union[bytes, AckableMessage]:
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
    redis_sentinels: List[Tuple[str, int]],
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
    redis_sentinels: List[Tuple[str, int]],
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
    redis_sentinels: List[Tuple[str, int]],
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
    redis_sentinels: List[Tuple[str, int]],
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
