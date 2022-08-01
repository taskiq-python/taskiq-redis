from typing import Mapping, Optional, Union

from errors import RedisConnectionError
from redis.asyncio import ConnectionPool, Redis


class RedisClient:
    """Class to interact with redis."""

    def __init__(self, redis_url: str) -> None:
        """
        Initializes redis connection.

        :param redis_url: Redis URL to connect.
        """
        self.redis_pool = ConnectionPool.from_url(redis_url)

    async def close(self) -> None:
        """Closes redis connection."""
        await self.redis_pool.disconnect()

    async def hset(
        self,
        name: str,
        mapping: Mapping[Union[str, bytes], Union[bytes, float, int, str]],
    ) -> None:
        """
        Adds new key-value in a redis hashmap with name `name`.

        :param name: The name of the redis hashmap.
        :param mapping: Dictionary with key-value pairs.

        :raises RedisConnectionError: if redis is not available.
        """
        try:
            async with Redis(connection_pool=self.redis_pool) as redis:
                await redis.hset(name, mapping=mapping)
        except ConnectionError as exc:
            raise RedisConnectionError("Redis is unavailable") from exc

    async def hget(self, name: str, key: str) -> Optional[bytes]:
        """
        Gets value from the hashmap with the given name and key.

        :param name: The name of the hashmap.
        :param key: The key in the hashmap.

        :raises RedisConnectionError: if redis is not available.

        :returns: bytes.
        """
        try:
            async with Redis(connection_pool=self.redis_pool) as redis:
                return await redis.hget(name=name, key=key)
        except ConnectionError as exc:
            raise RedisConnectionError("Redis is unavailable") from exc

    async def exists(self, name: str) -> bool:
        """
        Returns whether the name exists.

        :param name: name of the hashmap.

        :raises RedisConnectionError: if redis is not available.

        :returns: True if name exists else False
        """
        try:
            async with Redis(connection_pool=self.redis_pool) as redis:
                return bool(await redis.exists(name))
        except ConnectionError as exc:
            raise RedisConnectionError("Redis is unavailable") from exc
