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
import asyncio

from taskiq_redis.redis_broker import ListQueueBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

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
    print(await task.get_result())


asyncio.run(main())
```

## PubSubBroker and ListQueueBroker configuration

We have two brokers with similar interfaces, but with different logic.
The PubSubBroker uses redis' pubsub mechanism and is very powerful,
but it executes every task on all workers, because PUBSUB broadcasts message
to all subscribers.

If you want your messages to be processed only once, please use ListQueueBroker.
It uses redis' [LPUSH](https://redis.io/commands/lpush/) and [BRPOP](https://redis.io/commands/brpop/) commands to deal with messages.

Brokers parameters:
* `url` - url to redis.
* `task_id_generator` - custom task_id genertaor.
* `result_backend` - custom result backend.
* `queue_name` - name of the pub/sub channel in redis.
* `max_connection_pool_size` - maximum number of connections in pool.

## RedisAsyncResultBackend configuration

RedisAsyncResultBackend parameters:
* `redis_url` - url to redis.
* `keep_results` - flag to not remove results from Redis after reading.
* `result_ex_time` - expire time in seconds (by default - not specified)
* `result_px_time` - expire time in milliseconds (by default - not specified)
> IMPORTANT: **It is highly recommended to use expire time ​​in RedisAsyncResultBackend**  
> If you want to add expiration, either `result_ex_time` or `result_px_time` must be set.  
>```python
># First variant
>redis_async_result = RedisAsyncResultBackend(
>    redis_url="redis://localhost:6379",
>    result_ex_time=1000,
>)
>
># Second variant
>redis_async_result = RedisAsyncResultBackend(
>    redis_url="redis://localhost:6379",
>    result_px_time=1000000,
>)
>```