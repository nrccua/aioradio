"""aioradio redis cache script."""

# pylint: disable=c-extension-no-member
# pylint: disable=too-many-instance-attributes

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List

import aioredis
import orjson
from fakeredis.aioredis import create_redis_pool as fake_redis_pool

HASH_ALGO_MAP = {
    'SHA1': hashlib.sha1,
    'SHA224': hashlib.sha224,
    'SHA256': hashlib.sha256,
    'SHA384': hashlib.sha384,
    'SHA512': hashlib.sha512,
    'SHA3_224': hashlib.sha3_224,
    'SHA3_256': hashlib.sha3_256,
    'SHA3_384': hashlib.sha3_384,
    'SHA3_512': hashlib.sha3_512
}


@dataclass
class Redis:
    """class dealing with aioredis functions."""

    config: Dict[str, Any] = field(default_factory=dict)
    pool: aioredis.Redis = field(init=False, repr=False)
    pool_task: asyncio.coroutine = None

    # Set the redis pool min and max connections size
    pool_minsize: int = 5
    pool_maxsize: int = 10

    # Cache expiration in seconds
    expire: int = 60

    # If the key contains sensitive info than you can hash the cache key using various hash algorithms
    use_hashkey: bool = False
    hash_algorithm: str = 'SHA3_256'

    # If you want to pass in an object and let this class convert to json or
    # retrieve value letting this class convert from json set use_json = True.
    use_json: bool = True

    # used exclusively for pytest
    fakeredis: bool = False

    def __post_init__(self):
        if self.fakeredis:
            self.pool = asyncio.get_event_loop().run_until_complete(fake_redis_pool())
        else:
            primary_endpoint = f'redis://{self.config["redis_primary_endpoint"]}'
            loop = asyncio.get_event_loop()
            if loop and loop.is_running():
                self.pool_task = loop.create_task(
                    aioredis.create_redis_pool(primary_endpoint, minsize=self.pool_minsize, maxsize=self.pool_maxsize))
            else:
                self.pool = loop.run_until_complete(
                    aioredis.create_redis_pool(primary_endpoint, minsize=self.pool_minsize, maxsize=self.pool_maxsize))

    def __del__(self):
        self.pool.close()

    async def get_one_item(self, cache_key: str, use_json: bool=None) -> Any:
        """Check if an item is cached in redis.

        Args:
            cache_key (str): redis cache key
            use_json (bool, optional): convert json value to object. Defaults to None.

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        value = await self.pool.get(cache_key)

        if value is not None and use_json:
            value = orjson.loads(value)

        return value

    async def get_many_items(self, items: List[str], use_json: bool=None) -> List[Any]:
        """Check if many items are cached in redis.

        Args:
            items (List[str]): list of redis cache keys
            use_json (bool, optional): convert json values to objects. Defaults to None.

        Returns:
            List[Any]: list of objects
        """

        if use_json is None:
            use_json = self.use_json

        values = await self.pool.mget(*items)

        if use_json:
            values = [orjson.loads(val) if val is not None else None for val in values]

        return values

    async def set_one_item(self, cache_key: str, cache_value: str, expire: int=None, use_json: bool=None):
        """Set one key-value pair in redis.

        Args:
            cache_key (str): redis cache key
            cache_value (str): redis cache value
            expire (int, optional): cache expiration. Defaults to None.
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            cache_value = orjson.dumps(cache_value)

        await self.pool.set(cache_key, cache_value, expire=expire)

    async def delete_one_item(self, cache_key: str) -> int:
        """Delete key from redis.

        Args:
            cache_key (str): redis cache key

        Returns:
            int: 1 if key is found and deleted else 0
        """

        return await self.pool.delete(cache_key)

    async def build_cache_key(self, payload: Dict[str, Any], separator='|', use_hashkey: bool=None) -> str:
        """build a cache key from a dictionary object. Concatenate and
        normalize key-values from an unnested dict, taking care of sorting the
        keys and each of their values (if a list).

        Args:
            payload (Dict[str, Any]): dict object to use to build cache key
            separator (str, optional): character to use as a separator in the cache key. Defaults to '|'.
            use_hashkey (bool, optional): use a hashkey for the cache key. Defaults to None.

        Returns:
            str: [description]
        """

        if use_hashkey is None:
            use_hashkey = self.use_hashkey

        keys = sorted(payload.keys())
        concat_key = separator.join([f'{k}={orjson.dumps(payload[k], option=orjson.OPT_SORT_KEYS).decode()}'.replace('"', '')
                                    for k in keys if payload[k] != [] and payload[k] is not None])

        if use_hashkey:
            concat_key = HASH_ALGO_MAP[self.hash_algorithm](concat_key.encode()).hexdigest()

        return concat_key
