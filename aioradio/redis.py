"""aioradio redis cache script."""

# pylint: disable=c-extension-no-member
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes

import asyncio
import hashlib
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, List, Union

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

    config: Dict[str, Any] = dataclass_field(default_factory=dict)
    pool: aioredis.Redis = dataclass_field(init=False, repr=False)
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

    async def get(self, key: str, use_json: bool=None, encoding: Union[str, None]='utf-8') -> Any:
        """Check if an item is cached in redis.

        Args:
            key (str): redis cache key
            use_json (bool, optional): convert json value to object. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        value = await self.pool.get(key, encoding=encoding)

        if value is not None and use_json:
            value = orjson.loads(value)

        return value

    async def mget(self, items: List[str], use_json: bool=None, encoding: Union[str, None]='utf-8') -> List[Any]:
        """Check if many items are cached in redis.

        Args:
            items (List[str]): list of redis cache keys
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: list of objects
        """

        if use_json is None:
            use_json = self.use_json

        values = await self.pool.mget(*items, encoding=encoding)

        if use_json:
            values = [orjson.loads(val) if val is not None else None for val in values]

        return values

    async def set(self, key: str, value: str, expire: int=None, use_json: bool=None) -> int:
        """Set one key-value pair in redis.

        Args:
            key (str): redis cache key
            value (str): redis cache value
            expire (int, optional): cache expiration. Defaults to None.
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.

        Returns:
            int: 1 if key is set successfully else 0
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            value = orjson.dumps(value)

        return await self.pool.set(key, value, expire=expire)

    async def delete(self, key: str) -> int:
        """Delete key from redis.

        Args:
            key (str): redis cache key

        Returns:
            int: 1 if key is found and deleted else 0
        """

        return await self.pool.delete(key)

    async def hget(self, key: str, field: str, use_json: bool=None, encoding: Union[str, None]='utf-8') -> Any:
        """Get the value of a hash field.

        Args:
            key (str): cache key
            field (str): hash field
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        value = await self.pool.hget(key, field, encoding=encoding)

        if value is not None and use_json:
            value = orjson.loads(value)

        return value

    async def hmget(self, key: str, fields: List[str], use_json: bool=None, encoding: Union[str, None]='utf-8') -> Any:
        """Get the values of all the given fields.

        Args:
            key (str): cache key
            fields (List[str]): hash fields
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        items = {}
        for index, value in enumerate(await self.pool.hmget(key, *fields, encoding=encoding)):
            if value is not None:
                if use_json:
                    value = orjson.loads(value)
                items[fields[index]] = value

        return items

    async def hmget_many(self, keys: List[str], fields: List[str], use_json: bool=None, encoding: Union[str, None]='utf-8') -> List[Any]:
        """Get the values of all the given fields for many hashed keys.

        Args:
            keys (List[str]): cache keys
            fields (List[str]): hash fields
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: any
        """

        if use_json is None:
            use_json = self.use_json

        transaction = self.pool.multi_exec()
        for key in keys:
            transaction.hmget(key, *fields, encoding=encoding)

        results = []
        for values in await transaction.execute():
            items = {}
            for index, value in enumerate(values):
                if value is not None:
                    if use_json:
                        value = orjson.loads(value)
                    items[fields[index]] = value
            results.append(items)

        return results

    async def hgetall(self, key: str, use_json: bool=None, encoding: Union[str, None]='utf-8') -> Any:
        """Get all the fields and values in a hash.

        Args:
            key (str): cache key
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            Any: any
        """

        if use_json is None:
            use_json = self.use_json

        items = {}
        for hash_key, value in (await self.pool.hgetall(key, encoding=encoding)).items():
            if value is not None:
                if use_json:
                    value = orjson.loads(value)
                items[hash_key] = value

        return items

    async def hgetall_many(self, keys: List[str], use_json: bool=None, encoding: Union[str, None]='utf-8') -> List[Any]:
        """Get all the fields and values in a hash.

        Args:
            keys (str): cache keys
            use_json (bool, optional): convert json values to objects. Defaults to None.
            encoding (Union[str, None], optional): encoding of values

        Returns:
            List[Any]: any
        """

        if use_json is None:
            use_json = self.use_json

        transaction = self.pool.multi_exec()
        for key in keys:
            transaction.hgetall(key, encoding=encoding)

        results = []
        for item in await transaction.execute():
            items = {}
            for key, value in item.items():
                if value is not None:
                    if use_json:
                        value = orjson.loads(value)
                    items[key] = value
            results.append(items)

        return results

    async def hset(self, key: str, field: str, value: str, use_json: bool=None, expire: int=None) -> int:
        """Set the string value of a hash field.

        Args:
            key (str): cache key
            field (str): hash field
            value (str): hash value
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.
            expire (int, optional): cache expiration. Defaults to None.

        Returns:
            int: 1 if key is set successfully else 0
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            value = orjson.dumps(value)

        result = await self.pool.hset(key, field, value)
        await self.pool.expire(key, timeout=expire)

        return result

    async def hmset(self, key: str, items: Dict[str, Any], use_json: bool=None, expire: int=None) -> bool:
        """Set the string value of a hash field.

        Args:
            key (str): cache key
            items (List[str, Any]): list of redis hash key-value pairs
            use_json (bool, optional): set object to json before writing to cache. Defaults to None.
            expire (int, optional): cache expiration. Defaults to None.

        Returns:
            bool: True if hash is set successfully else False
        """

        if expire is None:
            expire = self.expire

        if use_json is None:
            use_json = self.use_json

        if use_json:
            modified_items = {k: orjson.dumps(v) for k, v in items.items()}
            items = modified_items

        result = await self.pool.hmset_dict(key, items)
        await self.pool.expire(key, timeout=expire)

        return  result

    async def hdel(self, key: str, fields: List[str]) -> int:
        """Delete one or more hash fields.

        Args:
            key (str): hash key
            fields (List[str]): hash fields

        Returns:
            int: Number of hash fields deleted
        """

        return await self.pool.hdel(key, *fields)

    async def hexists(self, key: str, field: str) -> bool:
        """Determine if hash field exists.

        Args:
            key (str): hash key
            field (str): hash field

        Returns:
            int: True if hash field exists else False
        """

        return await self.pool.hexists(key, field)

    async def build_cache_key(self, payload: Dict[str, Any], separator='|', use_hashkey: bool=None) -> str:
        """build a cache key from a dictionary object. Concatenate and
        normalize key-values from an unnested dict, taking care of sorting the
        keys and each of their values (if a list).

        Args:
            payload (Dict[str, Any]): dict object to use to build cache key
            separator (str, optional): character to use as a separator in the cache key. Defaults to '|'.
            use_hashkey (bool, optional): use a hashkey for the cache key. Defaults to None.

        Returns:
            str: dict object converted to string
        """

        if use_hashkey is None:
            use_hashkey = self.use_hashkey

        keys = sorted(payload.keys())
        concat_key = separator.join([f'{k}={orjson.dumps(payload[k], option=orjson.OPT_SORT_KEYS).decode()}'.replace('"', '')
                                    for k in keys if payload[k] != [] and payload[k] is not None])

        if use_hashkey:
            concat_key = HASH_ALGO_MAP[self.hash_algorithm](concat_key.encode()).hexdigest()

        return concat_key
