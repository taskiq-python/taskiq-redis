import asyncio
import uuid
from typing import Literal

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from taskiq import BrokerMessage

from taskiq_redis import PubSubBroker


async def wait_for_subscribers(client: Redis, channel: str, count: int) -> None:
    """Wait for the server to observe the expected subscription count."""
    while (await client.pubsub_numsub(channel))[0][1] != count:
        await asyncio.sleep(0)


@pytest.mark.anyio
@pytest.mark.parametrize("termination", ["close", "cancel", "disconnect"])
async def test_pubsub_listener_releases_connection(
    redis_url: str,
    termination: Literal["close", "cancel", "disconnect"],
) -> None:
    channel = uuid.uuid4().hex
    client_name = f"taskiq-pubsub-{uuid.uuid4().hex}"
    first_payload = b"first"
    replacement_payload = b"replacement"
    broker = PubSubBroker(
        redis_url,
        queue_name=channel,
        max_connection_pool_size=1,
        timeout=0.1,
        client_name=client_name,
        retry=Retry(NoBackoff(), 0),
    )
    listener = broker.listen()
    read = asyncio.create_task(anext(listener))
    replacement = broker.listen()
    next_read: asyncio.Task[bytes] | None = None
    try:
        async with Redis.from_url(redis_url) as observer:
            await asyncio.wait_for(wait_for_subscribers(observer, channel, 1), 5)
            if termination == "close":
                await observer.publish(channel, first_payload)
                assert await asyncio.wait_for(read, 5) == first_payload
                await listener.aclose()
            elif termination == "cancel":
                read.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await read
            else:
                clients = await observer.client_list(_type="pubsub")
                client_id = next(
                    str(client["id"])
                    for client in clients
                    if client["name"] == client_name
                )
                await observer.client_kill_filter(_id=client_id)
                with pytest.raises(RedisConnectionError):
                    await asyncio.wait_for(read, 5)

            # This needs the only pool slot returned by the terminated listener.
            await broker.kick(
                BrokerMessage(
                    task_id=uuid.uuid4().hex,
                    task_name="cleanup-test",
                    message=b"next",
                    labels={},
                ),
            )
            await asyncio.wait_for(wait_for_subscribers(observer, channel, 0), 5)

            next_read = asyncio.create_task(anext(replacement))
            await asyncio.wait_for(wait_for_subscribers(observer, channel, 1), 5)
            await observer.publish(channel, replacement_payload)
            assert await asyncio.wait_for(next_read, 5) == replacement_payload
            await replacement.aclose()
            await asyncio.wait_for(wait_for_subscribers(observer, channel, 0), 5)
    finally:
        read.cancel()
        await asyncio.gather(read, return_exceptions=True)
        if next_read is not None:
            next_read.cancel()
            await asyncio.gather(next_read, return_exceptions=True)
        await listener.aclose()
        await replacement.aclose()
        await broker.shutdown()
