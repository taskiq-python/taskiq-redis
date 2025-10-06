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

from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

result_backend = RedisAsyncResultBackend(
    redis_url="redis://localhost:6379",
)

# Or you can use PubSubBroker if you need broadcasting
# Or ListQueueBroker if you don't want acknowledges
broker = RedisStreamBroker(
    url="redis://localhost:6379",
).with_result_backend(result_backend)


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


## Schedule sources


You can use this package to add dynamic schedule sources. They are used to store
schedules for taskiq scheduler.

The advantage of using schedule sources from this package over default `LabelBased` source is that you can
dynamically add schedules in it.

We have two types of schedules:

* `RedisScheduleSource`
* `ListRedisScheduleSource`


### RedisScheduleSource

This source is super simple. It stores all schedules by key `{prefix}:{schedule_id}`. When scheduler requests
schedules, it retrieves all values from redis that start with a given `prefix`.

This is very ineficent and should not be used for high-volume schedules. Because if you have `1000` schedules, this scheduler will make at least `20` requests to retrieve them (we use `scan` and `mget` to minimize number of calls).

### ListRedisScheduleSource

This source holds values in lists.

* For cron tasks it uses key `{prefix}:cron`.
* For timed schedules it uses key `{prefix}:time:{time}` where `{time}` is actually time where schedules should run.

The main advantage of this approach is that we only fetch tasks we need to run at a given time and do not perform any excesive calls to redis.


### Migration from one source to another

To migrate from `RedisScheduleSource` to `ListRedisScheduleSource` you can define the latter as this:

```python
# broker.py
import asyncio
import datetime

from taskiq import TaskiqScheduler

from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker
from taskiq_redis.schedule_source import RedisScheduleSource

broker = RedisStreamBroker(url="redis://localhost:6379")

old_source = RedisScheduleSource("redis://localhost/1", prefix="prefix1")
array_source = ListRedisScheduleSource(
    "redis://localhost/1",
    prefix="prefix2",
    # To migrate schedules from an old source.
).with_migrate_from(
    old_source,
    # To delete schedules from an old source.
    delete_schedules=True,
)
scheduler = TaskiqScheduler(broker, [array_source])
```

During startup the scheduler will try to migrate schedules from an old source to a new one. Please be sure to specify different prefixe just to avoid any kind of collision between these two.


## Dynamic queue names


Brokers supports dynamic queue names, allowing you to specify different queues when kicking tasks. This is useful for routing tasks to specific queues based on runtime conditions, such as priority levels, tenant isolation, or environment-specific processing.

Simply pass the desired queue name as message's label when kicking a task to override the broker's default queue configuration.

```python
@broker.task(queue_name="low_priority")
async def low_priority_task() -> None:
    print("I don't mind waiting a little longer")
```
