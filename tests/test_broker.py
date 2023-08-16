import asyncio
import uuid
from typing import Union

import pytest
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from taskiq_redis import ListQueueBroker, PubSubBroker


def test_no_url_should_raise_typeerror() -> None:
    """Test that url is expected."""
    with pytest.raises(TypeError):
        ListQueueBroker()  # type: ignore

    with pytest.raises(TypeError):
        # it's `url`, not `redis_url`
        ListQueueBroker(redis_url="redis://localhost/0")  # type: ignore


async def get_message(  # type: ignore
    broker: AsyncBroker,
) -> Union[bytes, AckableMessage]:
    """
    Get a message from the broker.

    :param broker: async message broker.
    :return: first message from listen method.
    """
    async for message in broker.listen():  # noqa: WPS328
        return message


@pytest.fixture
def valid_broker_message() -> BrokerMessage:
    """
    Generate valid broker message for tests.

    :returns: broker message.
    """
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name=uuid.uuid4().hex,
        message="my_msg",
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
