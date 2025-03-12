# TaskIQ-Redis

Taskiq-redis is a plugin for taskiq that adds a new broker and result backend based on redis.

# Installation

To use this project you must have installed core taskiq library:
```bash
pip install taskiq
```
This project can be installed using pip:
```bash
pip install taskiq-redis
```

# Usage

Let's see the example with the redis broker and redis async result:

```python
# broker.py
import asyncio

from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

redis_async_result = RedisAsyncResultBackend(
    redis_url="redis://localhost:6379",
)

# Or you can use PubSubBroker if you need broadcasting
broker = ListQueueBroker(
    url="redis://localhost:6379",
    result_backend=redis_async_result,
)


@broker.task
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(5.5)
    print("All problems are solved!")


async def main():
    task = await best_task_ever.kiq()
    print(await task.wait_result())


if __name__ == "__main__":
    asyncio.run(main())
```

Launch the workers:
`taskiq worker broker:broker`
Then run the main code:
`python3 broker.py`


## Brokers

This package contains 6 broker implementations.
3 broker types:
* PubSub broker
* ListQueue broker
* Stream broker

Each of type is implemented for each redis architecture:
* Single node
* Cluster
* Sentinel

Here's a small breakdown of how they differ from eachother.


### PubSub

By default on old redis versions PUBSUB was the way of making redis into a queue.
But using PUBSUB means that all messages delivered to all subscribed consumers.

> [!WARNING]
> This broker doesn't support acknowledgements. If during message processing
> Worker was suddenly killed the message is going to be lost.

### ListQueue

This broker creates a list of messages at some key. Adding new tasks will be done
by appending them from the left side using `lpush`, and taking them from the right side using `brpop`.

> [!WARNING]
> This broker doesn't support acknowledgements. If during message processing
> Worker was suddenly killed the message is going to be lost.

### Stream

Stream brokers use redis [stream type](https://redis.io/docs/latest/develop/data-types/streams/) to store and fetch messages.

> [!TIP]
> This broker **supports** acknowledgements and therefore is fine to use in cases when data durability is
> required.

## RedisAsyncResultBackend configuration

RedisAsyncResultBackend parameters:
* `redis_url` - url to redis.
* `keep_results` - flag to not remove results from Redis after reading.
* `result_ex_time` - expire time in seconds (by default - not specified)
* `result_px_time` - expire time in milliseconds (by default - not specified)
* Any other keyword arguments are passed to `redis.asyncio.BlockingConnectionPool`.
  Notably, you can use `timeout` to set custom timeout in seconds for reconnects
  (or set it to `None` to try reconnects indefinitely).

> [!WARNING]
> **It is highly recommended to use expire time in RedisAsyncResultBackend**
> If you want to add expiration, either `result_ex_time` or `result_px_time` must be set.
> ```python
> # First variant
> redis_async_result = RedisAsyncResultBackend(
>     redis_url="redis://localhost:6379",
>     result_ex_time=1000,
> )
>
> # Second variant
> redis_async_result = RedisAsyncResultBackend(
>     redis_url="redis://localhost:6379",
>     result_px_time=1000000,
> )
> ```
